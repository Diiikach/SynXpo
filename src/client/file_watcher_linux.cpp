#include "synxpo/client/file_watcher.h"

#include <sys/inotify.h>
#include <unistd.h>
#include <poll.h>

#include <condition_variable>
#include <cstring>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace synxpo {

class LinuxFileWatcherImpl : public FileWatcher::Impl {
public:
    LinuxFileWatcherImpl(FileWatcher* owner) : Impl(owner) {}

    ~LinuxFileWatcherImpl() override {
        if (watch_thread_.joinable()) {
            watch_thread_.join();
        }
        if (inotify_fd_ != -1) {
            close(inotify_fd_);
        }
    }

    void StartImpl() override {
        init_error_.clear();
        init_done_ = false;
        
        watch_thread_ = std::thread([this]() { WatchThread(); });
        
        std::unique_lock<std::mutex> lock(init_mutex_);
        init_cv_.wait(lock, [this]() { return init_done_; });
        
        if (!init_error_.empty()) {
            if (watch_thread_.joinable()) {
                watch_thread_.join();
            }
            throw std::runtime_error(init_error_);
        }
    }

    void StopImpl() override {
        // running_ is already set to false by FileWatcher::Stop()
        // The watch loop will exit on next poll timeout.
        if (watch_thread_.joinable()) {
            watch_thread_.join();
        }
    }

    void AddWatchImpl(const std::filesystem::path& path, bool recursive) override {
        // Unfortunately, inotify api is not thread-safe. We can't just call inotify_add_watch,
        // all initialization and polling must be done in one thread.
        // So we just save the watch to add it later.
        pending_watches_[path] = recursive;
    }

    void RemoveWatchImpl(const std::filesystem::path& path) override {
        pending_watches_.erase(path);
    }

private:
    void AddWatchRecursive(const std::filesystem::path& path, bool recursive) {
        uint32_t mask = IN_CREATE | IN_MODIFY | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO;
        
        int wd = inotify_add_watch(inotify_fd_, path.c_str(), mask);
        if (wd == -1) {
            throw std::runtime_error("Failed to add watch for " + path.string() + 
                                   ": " + std::string(std::strerror(errno)));
        }

        wd_to_path_[wd] = path;
        path_to_wd_[path] = wd;

        if (recursive && std::filesystem::is_directory(path)) {
            try {
                for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
                    if (entry.is_directory()) {
                        int sub_wd = inotify_add_watch(inotify_fd_, entry.path().c_str(), mask);
                        if (sub_wd != -1) {
                            wd_to_path_[sub_wd] = entry.path();
                            path_to_wd_[entry.path()] = sub_wd;
                        }
                    }
                }
            } catch (const std::filesystem::filesystem_error& e) {
                // Ignore permission errors
            }
        }
    }

    void WatchThread() {
        inotify_fd_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        
        if (inotify_fd_ == -1) {
            {
                std::lock_guard<std::mutex> lock(init_mutex_);
                init_error_ = "Failed to initialize inotify: " + std::string(std::strerror(errno));
                init_done_ = true;
            }
            init_cv_.notify_one();
            return;
        }
        
        for (const auto& [path, recursive] : pending_watches_) {
            try {
                AddWatchRecursive(path, recursive);
            } catch (const std::exception& e) {
                {
                    std::lock_guard<std::mutex> lock(init_mutex_);
                    init_error_ = "Failed to add watch: " + std::string(e.what());
                    init_done_ = true;
                }
                init_cv_.notify_one();
                close(inotify_fd_);
                inotify_fd_ = -1;
                return;
            }
        }
        
        {
            std::lock_guard<std::mutex> lock(init_mutex_);
            init_done_ = true;
        }
        init_cv_.notify_one();
        
        WatchLoop();

        if (inotify_fd_ != -1) {
            close(inotify_fd_);
            inotify_fd_ = -1;
        }
        
        wd_to_path_.clear();
        path_to_wd_.clear();
        moved_from_.clear();
    }

    void WatchLoop() {
        constexpr size_t BUF_LEN = 4096;
        alignas(struct inotify_event) char buf[BUF_LEN];

        struct pollfd pfd;
        pfd.fd = inotify_fd_;
        pfd.events = POLLIN;

        while (IsOwnerRunning()) {
            int poll_result = poll(&pfd, 1, 100);  // 100ms timeout
            
            if (poll_result == -1) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }

            if (poll_result == 0) {
                continue;  // Timeout, check running flag
            }
            
            ssize_t len = read(inotify_fd_, buf, sizeof(buf));
            if (len == -1 && errno != EAGAIN) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }

            if (len <= 0) {
                continue;
            }

            for (char* ptr = buf; ptr < buf + len; ) {
                struct inotify_event* event = reinterpret_cast<struct inotify_event*>(ptr);
                ProcessEvent(event);
                ptr += sizeof(struct inotify_event) + event->len;
            }
        }
    }

    void ProcessEvent(struct inotify_event* event) {
        auto it = wd_to_path_.find(event->wd);
        if (it == wd_to_path_.end()) {
            return;
        }

        std::filesystem::path base_path = it->second;
        std::filesystem::path full_path = base_path / event->name;

        FileEvent file_event;
        file_event.timestamp = std::chrono::system_clock::now();
        file_event.path = full_path;
        file_event.entry_type = (event->mask & IN_ISDIR) ? FSEntryType::Directory : FSEntryType::File;

        if (event->mask & IN_CREATE) {
            file_event.type = FileEventType::Created;
            
            if (event->mask & IN_ISDIR) {
                try {
                    AddWatchRecursive(full_path, true);
                } catch (...) {
                    // Ignore errors
                }
            }
        } else if (event->mask & IN_MODIFY) {
            file_event.type = FileEventType::Modified;
        } else if (event->mask & IN_DELETE) {
            file_event.type = FileEventType::Deleted;
        } else if (event->mask & IN_MOVED_FROM) {
            moved_from_[event->cookie] = full_path;
            return;
        } else if (event->mask & IN_MOVED_TO) {
            auto moved_it = moved_from_.find(event->cookie);
            if (moved_it != moved_from_.end()) {
                file_event.type = FileEventType::Renamed;
                file_event.old_path = moved_it->second;
                moved_from_.erase(moved_it);
            } else {
                file_event.type = FileEventType::Created;
            }
            
            if (event->mask & IN_ISDIR) {
                try {
                    AddWatchRecursive(full_path, true);
                } catch (...) {
                    // Ignore errors
                }
            }
        } else {
            return;
        }

        auto& callback = GetCallback();
        if (callback) {
            callback(file_event);
        }
    }

    int inotify_fd_ = -1;
    std::thread watch_thread_;
    
    std::mutex init_mutex_;
    std::condition_variable init_cv_;
    bool init_done_ = false;
    std::string init_error_;
    
    std::map<std::filesystem::path, bool> pending_watches_;  // path -> recursive flag
    std::map<int, std::filesystem::path> wd_to_path_;        // watch descriptor -> path
    std::map<std::filesystem::path, int> path_to_wd_;        // path -> watch descriptor
    std::map<uint32_t, std::filesystem::path> moved_from_;   // cookie -> old path
};

FileWatcher::FileWatcher() {
    pimpl_ = std::unique_ptr<FileWatcher::Impl>(new LinuxFileWatcherImpl(this));
}

}  // namespace synxpo
