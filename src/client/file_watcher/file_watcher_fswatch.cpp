#include "synxpo/client/file_watcher.h"

#include <libfswatch/c++/monitor.hpp>
#include <libfswatch/c++/monitor_factory.hpp>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <stdexcept>
#include <thread>
#include <vector>

#include <absl/log/log.h>

namespace synxpo {

class FswatchFileWatcherImpl : public FileWatcher::Impl {
public:
    FswatchFileWatcherImpl(FileWatcher* owner) : Impl(owner) {}

    ~FswatchFileWatcherImpl() override {
        should_stop_ = true;
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
        should_stop_ = true;
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
    // Static callback wrapper for fswatch
    static void StaticEventCallback(const std::vector<fsw::event>& events, void* context) {
        auto* impl = static_cast<FswatchFileWatcherImpl*>(context);
        impl->ProcessEvents(events);
    }

    void WatchThread() {
        while (!should_stop_ && IsOwnerRunning()) {
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
                        CachePathType(path);

                        // Add all subdirectories
                        try {
                            for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
                                if (entry.is_directory()) {
                                    paths.push_back(entry.path().string());
                                    recursive_map[entry.path().string()] = true;
                                    CachePathType(entry.path());
                                }
                            }
                        } catch (const std::filesystem::filesystem_error&) {
                            // Ignore permission errors
                        }
                    } else {
                        paths.push_back(path.string());
                        recursive_map[path.string()] = false;
                        CachePathType(path);
                    }
                }
            }

            // Add any new directories discovered during monitoring
                {
                    std::lock_guard<std::mutex> lock(new_dirs_mutex_);
                    for (const auto& dir : new_directories_) {
                        paths.push_back(dir.string());
                    }
                    new_directories_.clear();
                }

                if (paths.empty()) {
                    std::lock_guard<std::mutex> lock(init_mutex_);
                    init_error_ = "No paths to watch";
                    init_done_ = true;
                    init_cv_.notify_one();
                    return;
                }

                // Create monitor with static callback function pointer
                fsw::monitor* raw_monitor = fsw::monitor_factory::create_monitor(
                    fsw_monitor_type::system_default_monitor_type,
                    paths,
                    &StaticEventCallback
                );

                if (!raw_monitor) {
                    std::lock_guard<std::mutex> lock(init_mutex_);
                    init_error_ = "Failed to create fswatch monitor";
                    init_done_ = true;
                    init_cv_.notify_one();
                    return;
                }

                // Wrap in unique_ptr
                monitor_.reset(raw_monitor);

                // Set context to this instance
                monitor_->set_context(this);

                // Configure monitor
                monitor_->set_recursive(false);  // We handle recursion manually
                monitor_->set_latency(0.1);      // 100ms latency

                {
                    std::lock_guard<std::mutex> lock(init_mutex_);
                    init_done_ = true;
                }
                init_cv_.notify_one();

                // Start monitoring (blocking call)
                // This will return when stop() is called
                monitor_->start();

                // Check if we need to restart due to new directories
                {
                    std::lock_guard<std::mutex> lock(new_dirs_mutex_);
                    if (new_directories_.empty() || should_stop_ || !IsOwnerRunning()) {
                        break;
                    }
                }

                // Clean up before restart
                monitor_.reset();

            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(init_mutex_);
                init_error_ = "Failed to start monitor: " + std::string(e.what());
                init_done_ = true;
                init_cv_.notify_one();
                break;
            }
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
            FSEntryType cached_type = FSEntryType::File;
            bool found_in_cache = false;

            {
                std::lock_guard<std::mutex> lock(path_types_mutex_);
                auto it = path_types_.find(file_event.path);
                if (it != path_types_.end()) {
                    cached_type = it->second;
                    found_in_cache = true;
                }
            }

            try {
                if (std::filesystem::exists(file_event.path)) {
                    file_event.entry_type = std::filesystem::is_directory(file_event.path)
                        ? FSEntryType::Directory
                        : FSEntryType::File;

                    // Cache the type for future reference
                    std::lock_guard<std::mutex> lock(path_types_mutex_);
                    path_types_[file_event.path] = file_event.entry_type;
                } else {
                    // If file doesn't exist, use cached type if available
                    file_event.entry_type = found_in_cache ? cached_type : FSEntryType::File;
                }
            } catch (...) {
                file_event.entry_type = found_in_cache ? cached_type : FSEntryType::File;
            }

            // Process event flags
            const auto& flags = fsw_event.get_flags();
            bool created = false;
            bool modified = false;
            bool deleted = false;
            bool renamed = false;

            // Log all flags for debugging
            std::string flags_str;
            for (const auto& flag : flags) {
                if (!flags_str.empty()) flags_str += ", ";
                switch (flag) {
                    case fsw_event_flag::NoOp: flags_str += "NoOp"; break;
                    case fsw_event_flag::PlatformSpecific: flags_str += "PlatformSpecific"; break;
                    case fsw_event_flag::Created: flags_str += "Created"; break;
                    case fsw_event_flag::Updated: flags_str += "Updated"; break;
                    case fsw_event_flag::Removed: flags_str += "Removed"; break;
                    case fsw_event_flag::Renamed: flags_str += "Renamed"; break;
                    case fsw_event_flag::OwnerModified: flags_str += "OwnerModified"; break;
                    case fsw_event_flag::AttributeModified: flags_str += "AttributeModified"; break;
                    case fsw_event_flag::MovedFrom: flags_str += "MovedFrom"; break;
                    case fsw_event_flag::MovedTo: flags_str += "MovedTo"; break;
                    case fsw_event_flag::IsFile: flags_str += "IsFile"; break;
                    case fsw_event_flag::IsDir: flags_str += "IsDir"; break;
                    case fsw_event_flag::IsSymLink: flags_str += "IsSymLink"; break;
                    case fsw_event_flag::Link: flags_str += "Link"; break;
                    case fsw_event_flag::Overflow: flags_str += "Overflow"; break;
                    default: flags_str += "Unknown"; break;
                }
            }
            LOG(INFO) << "[FileWatcher] fswatch event: path=" << file_event.path << " flags=[" << flags_str << "]";

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
            std::string event_type_str;
            if (deleted) {
                file_event.type = FileEventType::Deleted;
                event_type_str = "Deleted";

                // Clean up cached type after deletion
                std::lock_guard<std::mutex> lock(path_types_mutex_);
                path_types_.erase(file_event.path);
            } else if (renamed) {
                file_event.type = FileEventType::Renamed;
                event_type_str = "Renamed";

                // Note: fswatch doesn't provide old path directly
                // Try to match with MovedFrom event using path similarity
                std::lock_guard<std::mutex> lock(rename_mutex_);

                bool is_moved_from = false;
                bool is_moved_to = false;

                for (const auto& flag : flags) {
                    if (flag == fsw_event_flag::MovedFrom) {
                        is_moved_from = true;
                    } else if (flag == fsw_event_flag::MovedTo) {
                        is_moved_to = true;
                    }
                }

                if (is_moved_from) {
                    // Store this as a potential source of rename
                    auto now = std::chrono::system_clock::now();
                    pending_renames_by_name_[file_event.path.filename().string()].push_back({file_event.path, now});
                    pending_renames_global_[file_event.path] = now;
                    
                    // Prune old pending renames and collect expired paths
                    std::vector<std::filesystem::path> expired_paths;
                    PrunePendingRenames(now, expired_paths);
                    
                    // Emit deletion events for expired MovedFrom (file moved out of watched folder)
                    for (const auto& expired_path : expired_paths) {
                        FileEvent delete_event;
                        delete_event.timestamp = now;
                        delete_event.path = expired_path;
                        delete_event.type = FileEventType::Deleted;
                        delete_event.entry_type = FSEntryType::Unknown;
                        
                        LOG(INFO) << "[FileWatcher] Emitting deletion for orphaned MovedFrom: " << expired_path;
                        callback(delete_event);
                    }
                    
                    continue; // Don't emit event yet for current MovedFrom
                } else if (is_moved_to) {
                    // Try to find matching MovedFrom
                    auto now = std::chrono::system_clock::now();
                    std::filesystem::path old_path;

                    // Prefer match by filename if available
                    auto name_it = pending_renames_by_name_.find(file_event.path.filename().string());
                    if (name_it != pending_renames_by_name_.end()) {
                        auto& vec = name_it->second;
                        for (auto it = vec.begin(); it != vec.end(); ) {
                            if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() > 1) {
                                it = vec.erase(it);
                            } else {
                                old_path = it->first;
                                vec.erase(it);
                                break;
                            }
                        }
                        if (vec.empty()) {
                            pending_renames_by_name_.erase(name_it);
                        }
                    }

                    // Fallback: use any pending rename
                    if (old_path.empty()) {
                        for (auto it = pending_renames_global_.begin(); it != pending_renames_global_.end(); ) {
                            if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() > 1) {
                                it = pending_renames_global_.erase(it);
                            } else {
                                old_path = it->first;
                                pending_renames_global_.erase(it);
                                break;
                            }
                        }
                    }

                    if (!old_path.empty()) {
                        file_event.old_path = old_path;

                        // Update type cache with new path
                        std::lock_guard<std::mutex> type_lock(path_types_mutex_);
                        auto type_it = path_types_.find(old_path);
                        if (type_it != path_types_.end()) {
                            path_types_[file_event.path] = type_it->second;
                            path_types_.erase(type_it);
                        }
                    }
                }
            } else if (created) {
                file_event.type = FileEventType::Created;

                // If a new directory was created and we're watching recursively,
                // we need to add watch for it
                if (file_event.entry_type == FSEntryType::Directory) {
                    AddDirectoryWatch(file_event.path);
                }
                event_type_str = "Created";
            } else if (modified) {
                file_event.type = FileEventType::Modified;
                event_type_str = "Modified";
            } else {
                // Unknown event, skip
                LOG(INFO) << "[FileWatcher] Skipping unknown event for: " << file_event.path;
                continue;
            }

            LOG(INFO) << "[FileWatcher] Emitting event: path=" << file_event.path << " type=" << event_type_str;
            callback(file_event);
        }
        
        // After processing all events, check for any remaining expired MovedFrom events
        // This handles the case where a file is moved out and no other events follow
        {
            std::lock_guard<std::mutex> lock(rename_mutex_);
            auto now = std::chrono::system_clock::now();
            std::vector<std::filesystem::path> expired_paths;
            PrunePendingRenames(now, expired_paths);
            
            for (const auto& expired_path : expired_paths) {
                FileEvent delete_event;
                delete_event.timestamp = now;
                delete_event.path = expired_path;
                delete_event.type = FileEventType::Deleted;
                delete_event.entry_type = FSEntryType::Unknown;
                
                LOG(INFO) << "[FileWatcher] Emitting deletion for orphaned MovedFrom (end of batch): " << expired_path;
                callback(delete_event);
            }
        }
    }

    void AddDirectoryWatch(const std::filesystem::path& dir_path) {
        // fswatch doesn't support dynamically adding paths to an existing monitor
        // We need to track new directories and restart the monitor

        {
            std::lock_guard<std::mutex> lock(new_dirs_mutex_);
            new_directories_.insert(dir_path);
            CachePathType(dir_path);

            // Also add subdirectories recursively
            try {
                for (const auto& entry : std::filesystem::recursive_directory_iterator(dir_path)) {
                    if (entry.is_directory()) {
                        new_directories_.insert(entry.path());
                        CachePathType(entry.path());
                    }
                }
            } catch (const std::filesystem::filesystem_error&) {
                // Ignore permission errors
            }
        }

        // Trigger monitor restart
        if (monitor_) {
            monitor_->stop();
        }
    }

    void CachePathType(const std::filesystem::path& path) {
        try {
            FSEntryType type = std::filesystem::is_directory(path) ? FSEntryType::Directory : FSEntryType::File;
            std::lock_guard<std::mutex> lock(path_types_mutex_);
            path_types_[path] = type;
        } catch (...) {
            // Ignore if we can't stat
        }
    }

    void PrunePendingRenames(const std::chrono::system_clock::time_point& now,
                             std::vector<std::filesystem::path>& expired_paths) {
        constexpr auto kWindowSeconds = 1;
        for (auto it = pending_renames_global_.begin(); it != pending_renames_global_.end(); ) {
            if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() > kWindowSeconds) {
                expired_paths.push_back(it->first);
                it = pending_renames_global_.erase(it);
            } else {
                ++it;
            }
        }

        for (auto name_it = pending_renames_by_name_.begin(); name_it != pending_renames_by_name_.end(); ) {
            auto& vec = name_it->second;
            for (auto it = vec.begin(); it != vec.end(); ) {
                if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() > kWindowSeconds) {
                    // Note: path already added via pending_renames_global_
                    it = vec.erase(it);
                } else {
                    ++it;
                }
            }
            if (vec.empty()) {
                name_it = pending_renames_by_name_.erase(name_it);
            } else {
                ++name_it;
            }
        }
    }

    std::unique_ptr<fsw::monitor> monitor_;
    std::thread watch_thread_;

    std::mutex init_mutex_;
    std::condition_variable init_cv_;
    bool init_done_ = false;
    std::string init_error_;

    std::mutex watches_mutex_;
    std::map<std::filesystem::path, bool> watches_;  // path -> recursive flag

    std::mutex path_types_mutex_;
    std::map<std::filesystem::path, FSEntryType> path_types_;  // Cache of path types

    std::mutex rename_mutex_;
    std::map<std::filesystem::path, std::chrono::system_clock::time_point> pending_renames_global_;  // MovedFrom paths awaiting MovedTo (any name)
    std::map<std::string, std::vector<std::pair<std::filesystem::path, std::chrono::system_clock::time_point>>> pending_renames_by_name_;

    std::mutex new_dirs_mutex_;
    std::set<std::filesystem::path> new_directories_;  // New directories to watch

    std::atomic<bool> should_stop_{false};
};

FileWatcher::FileWatcher() {
    pimpl_ = std::unique_ptr<FileWatcher::Impl>(new FswatchFileWatcherImpl(this));
}

}  // namespace synxpo
