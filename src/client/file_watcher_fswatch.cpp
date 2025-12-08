
#include "synxpo/client/file_watcher.h"

#include <libfswatch/c++/monitor.hpp>
#include <libfswatch/c++/monitor_factory.hpp>

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace synxpo {

class FswatchFileWatcherImpl : public FileWatcher::Impl {
public:
    FswatchFileWatcherImpl(FileWatcher* owner) : Impl(owner) {}

    ~FswatchFileWatcherImpl() override {
        if (monitor_) {
            monitor_->stop();
        }
        if (watch_thread_.joinable()) {
            watch_thread_.join();
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
        if (monitor_) {
            monitor_->stop();
        }
        if (watch_thread_.joinable()) {
            watch_thread_.join();
        }
    }

    void AddWatchImpl(const std::filesystem::path& path, bool recursive) override {
        std::lock_guard<std::mutex> lock(watches_mutex_);
        watches_[path] = recursive;
    }

    void RemoveWatchImpl(const std::filesystem::path& path) override {
        std::lock_guard<std::mutex> lock(watches_mutex_);
        watches_.erase(path);
    }

private:
    void WatchThread() {
        try {
            std::vector<std::string> paths;
            std::map<std::string, bool> recursive_map;

            {
                std::lock_guard<std::mutex> lock(watches_mutex_);
                for (const auto& [path, recursive] : watches_) {
                    if (recursive && std::filesystem::is_directory(path)) {
                        // Add root directory
                        paths.push_back(path.string());
                        recursive_map[path.string()] = true;

                        // Add all subdirectories
                        try {
                            for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
                                if (entry.is_directory()) {
                                    paths.push_back(entry.path().string());
                                    recursive_map[entry.path().string()] = true;
                                }
                            }
                        } catch (const std::filesystem::filesystem_error&) {
                            // Ignore permission errors
                        }
                    } else {
                        paths.push_back(path.string());
                        recursive_map[path.string()] = false;
                    }
                }
            }

            if (paths.empty()) {
                std::lock_guard<std::mutex> lock(init_mutex_);
                init_error_ = "No paths to watch";
                init_done_ = true;
                init_cv_.notify_one();
                return;
            }

            // Create monitor
            monitor_ = fsw::monitor_factory::create_monitor(
                fsw_monitor_type::system_default_monitor_type,
                paths,
                [this](const std::vector<fsw::event>& events, void* context) {
                    this->ProcessEvents(events);
                }
            );

            if (!monitor_) {
                std::lock_guard<std::mutex> lock(init_mutex_);
                init_error_ = "Failed to create fswatch monitor";
                init_done_ = true;
                init_cv_.notify_one();
                return;
            }

            // Configure monitor
            monitor_->set_recursive(false);  // We handle recursion manually
            monitor_->set_latency(0.1);      // 100ms latency

            {
                std::lock_guard<std::mutex> lock(init_mutex_);
                init_done_ = true;
            }
            init_cv_.notify_one();

            // Start monitoring (blocking call)
            monitor_->start();

        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(init_mutex_);
            init_error_ = "Failed to start monitor: " + std::string(e.what());
            init_done_ = true;
            init_cv_.notify_one();
        }
    }

    void ProcessEvents(const std::vector<fsw::event>& events) {
        if (!IsOwnerRunning()) {
            return;
        }

        auto& callback = GetCallback();
        if (!callback) {
            return;
        }

        for (const auto& fsw_event : events) {
            FileEvent file_event;
            file_event.timestamp = std::chrono::system_clock::now();
            file_event.path = fsw_event.get_path();

            // Determine entry type
            try {
                if (std::filesystem::exists(file_event.path)) {
                    file_event.entry_type = std::filesystem::is_directory(file_event.path) 
                        ? FSEntryType::Directory 
                        : FSEntryType::File;
                } else {
                    // If file doesn't exist, it was likely deleted
                    file_event.entry_type = FSEntryType::File;
                }
            } catch (...) {
                file_event.entry_type = FSEntryType::File;
            }

            // Process event flags
            const auto& flags = fsw_event.get_flags();
            bool created = false;
            bool modified = false;
            bool deleted = false;
            bool renamed = false;

            for (const auto& flag : flags) {
                switch (flag) {
                    case fsw_event_flag::Created:
                        created = true;
                        break;
                    case fsw_event_flag::Updated:
                    case fsw_event_flag::AttributeModified:
                        modified = true;
                        break;
                    case fsw_event_flag::Removed:
                        deleted = true;
                        break;
                    case fsw_event_flag::Renamed:
                    case fsw_event_flag::MovedFrom:
                    case fsw_event_flag::MovedTo:
                        renamed = true;
                        break;
                    default:
                        break;
                }
            }

            // Determine event type (priority: deleted > renamed > created > modified)
            if (deleted) {
                file_event.type = FileEventType::Deleted;
            } else if (renamed) {
                file_event.type = FileEventType::Renamed;
                // Note: fswatch doesn't provide old path, so old_path remains empty
            } else if (created) {
                file_event.type = FileEventType::Created;

                // If a new directory was created and we're watching recursively,
                // we need to restart the monitor to include it
                if (file_event.entry_type == FSEntryType::Directory) {
                    ScheduleMonitorRestart();
                }
            } else if (modified) {
                file_event.type = FileEventType::Modified;
            } else {
                // Unknown event, skip
                continue;
            }

            callback(file_event);
        }
    }

    void ScheduleMonitorRestart() {
        // For simplicity, we don't restart the monitor automatically
        // This would require stopping and restarting in a separate thread
        // Most use cases don't require immediate subdirectory watching
    }

    std::unique_ptr<fsw::monitor> monitor_;
    std::thread watch_thread_;

    std::mutex init_mutex_;
    std::condition_variable init_cv_;
    bool init_done_ = false;
    std::string init_error_;

    std::mutex watches_mutex_;
    std::map<std::filesystem::path, bool> watches_;  // path -> recursive flag
};

FileWatcher::FileWatcher() {
    pimpl_ = std::unique_ptr<FileWatcher::Impl>(new FswatchFileWatcherImpl(this));
}

}  // namespace synxpo