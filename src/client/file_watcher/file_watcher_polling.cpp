#include "synxpo/client/file_watcher.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace synxpo {

namespace {

struct EntryInfo {
    fs::file_time_type write_time{};
    bool is_directory = false;
};

using Snapshot = std::unordered_map<fs::path, EntryInfo>;

Snapshot BuildSnapshot(const std::vector<std::pair<fs::path, bool>>& watches) {
    Snapshot snapshot;

    for (const auto& [watch_path, recursive] : watches) {
        if (!fs::exists(watch_path)) {
            continue;
        }

        auto add_entry = [&](const fs::path& p, bool is_dir) {
            EntryInfo info{};
            info.is_directory = is_dir;
            if (!is_dir) {
                std::error_code ec;
                info.write_time = fs::last_write_time(p, ec);
                if (ec) {
                    return;
                }
            }
            snapshot.emplace(p, info);
        };

        if (fs::is_directory(watch_path)) {
            add_entry(watch_path, true);

            if (recursive) {
                for (auto it = fs::recursive_directory_iterator(
                         watch_path, fs::directory_options::skip_permission_denied);
                     it != fs::recursive_directory_iterator(); ++it) {
                    const fs::directory_entry& entry = *it;
                    std::error_code ec;
                    bool is_dir = entry.is_directory(ec);
                    if (ec) {
                        continue;
                    }
                    add_entry(entry.path(), is_dir);
                }
            } else {
                for (auto it = fs::directory_iterator(
                         watch_path, fs::directory_options::skip_permission_denied);
                     it != fs::directory_iterator(); ++it) {
                    const fs::directory_entry& entry = *it;
                    std::error_code ec;
                    bool is_dir = entry.is_directory(ec);
                    if (ec) {
                        continue;
                    }
                    add_entry(entry.path(), is_dir);
                }
            }
        } else {
            add_entry(watch_path, false);
        }
    }

    return snapshot;
}

FSEntryType ToEntryType(const EntryInfo& info) {
    if (info.is_directory) {
        return FSEntryType::Directory;
    }
    return FSEntryType::File;
}

}  // namespace

class PollingFileWatcherImpl : public FileWatcher::Impl {
public:
    explicit PollingFileWatcherImpl(FileWatcher* owner)
        : FileWatcher::Impl(owner) {}

    ~PollingFileWatcherImpl() override { StopImpl(); }

    void StartImpl() override {
        std::lock_guard<std::mutex> lock(watches_mutex_);
        if (watch_thread_.joinable()) {
            return;
        }

        stop_requested_.store(false);

        // Capture initial snapshot so we only emit changes that happen after Start.
        previous_snapshot_ = BuildSnapshot(watches_);

        watch_thread_ = std::thread([this]() { WatchLoop(); });
    }

    void StopImpl() override {
        {
            std::lock_guard<std::mutex> lock(watches_mutex_);
            stop_requested_.store(true);
        }

        if (watch_thread_.joinable()) {
            watch_thread_.join();
        }
    }

    void AddWatchImpl(const fs::path& path, bool recursive) override {
        std::lock_guard<std::mutex> lock(watches_mutex_);
        watches_.emplace_back(path, recursive);
    }

    void RemoveWatchImpl(const fs::path& path) override {
        std::lock_guard<std::mutex> lock(watches_mutex_);
        watches_.erase(
            std::remove_if(
                watches_.begin(), watches_.end(),
                [&](const auto& entry) { return entry.first == path; }),
            watches_.end());
    }

private:
    void WatchLoop() {
        while (IsOwnerRunning() && !stop_requested_.load()) {
            Snapshot current_snapshot;
            {
                std::lock_guard<std::mutex> lock(watches_mutex_);
                current_snapshot = BuildSnapshot(watches_);
            }

            EmitDiff(previous_snapshot_, current_snapshot);
            previous_snapshot_ = std::move(current_snapshot);

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void EmitDiff(const Snapshot& old_snapshot, const Snapshot& new_snapshot) {
        auto now = std::chrono::system_clock::now();
        auto& callback = GetCallback();

        // Created or modified
        for (const auto& [path, info] : new_snapshot) {
            auto it = old_snapshot.find(path);
            if (it == old_snapshot.end()) {
                if (callback) {
                    callback(FileEvent{
                        .type = FileEventType::Created,
                        .entry_type = ToEntryType(info),
                        .path = path,
                        .old_path = std::nullopt,
                        .timestamp = now});
                }
            } else if (!info.is_directory && info.write_time != it->second.write_time) {
                if (callback) {
                    callback(FileEvent{
                        .type = FileEventType::Modified,
                        .entry_type = ToEntryType(info),
                        .path = path,
                        .old_path = std::nullopt,
                        .timestamp = now});
                }
            }
        }

        // Deleted
        for (const auto& [path, info] : old_snapshot) {
            if (new_snapshot.find(path) == new_snapshot.end()) {
                if (callback) {
                    callback(FileEvent{
                        .type = FileEventType::Deleted,
                        .entry_type = ToEntryType(info),
                        .path = path,
                        .old_path = std::nullopt,
                        .timestamp = now});
                }
            }
        }
    }

    std::vector<std::pair<fs::path, bool>> watches_;
    Snapshot previous_snapshot_;
    std::thread watch_thread_;
    std::mutex watches_mutex_;
    std::atomic<bool> stop_requested_{false};
};

FileWatcher::FileWatcher() {
    pimpl_ = std::make_unique<PollingFileWatcherImpl>(this);
}

}  // namespace synxpo

