#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>

namespace synxpo {

enum class FileEventType {
    Created,
    Modified,
    Deleted,
    Renamed
};

enum class FSEntryType {
    File,
    Directory,
    Unknown
};

struct FileEvent {
    FileEventType type;
    FSEntryType entry_type;
    std::filesystem::path path;
    std::optional<std::filesystem::path> old_path;
    std::chrono::system_clock::time_point timestamp;
};

using FileEventCallback = std::function<void(const FileEvent&)>;

class FileWatcher {
public:
    FileWatcher();
    ~FileWatcher();

    FileWatcher(const FileWatcher&) = delete;
    FileWatcher& operator=(const FileWatcher&) = delete;
    FileWatcher(FileWatcher&&) = delete;
    FileWatcher& operator=(FileWatcher&&) = delete;

    // Add a directory to watch
    // recursive - whether to watch subdirectories
    void AddWatch(const std::filesystem::path& path, bool recursive = true);

    // Remove a directory from watching
    void RemoveWatch(const std::filesystem::path& path);

    // Set callback for file events
    void SetEventCallback(FileEventCallback callback);

    // Start watching (asynchronously)
    void Start();

    // Stop watching
    void Stop();

    // Check if watcher is running
    bool IsRunning() const;

    class Impl {
    public:
        explicit Impl(FileWatcher* owner) : owner_(owner) {}
        virtual ~Impl() = default;
        
        virtual void StartImpl() = 0;
        virtual void StopImpl() = 0;
        virtual void AddWatchImpl(const std::filesystem::path& path, bool recursive) = 0;
        virtual void RemoveWatchImpl(const std::filesystem::path& path) = 0;
        
    protected:
        FileEventCallback& GetCallback();
        bool IsOwnerRunning() const;
        
        FileWatcher* owner_;
    };

private:
    std::unique_ptr<Impl> pimpl_;
    
    FileEventCallback callback_;
    std::atomic<bool> running_{false};
    
    friend class Impl;
};

}  // namespace synxpo
