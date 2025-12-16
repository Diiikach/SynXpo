#include "synxpo/client/file_watcher.h"
#include "synxpo/client/logger.h"

namespace synxpo {

FileWatcher::~FileWatcher() {
    if (running_) {
        Stop();
    }
}

void FileWatcher::AddWatch(const std::filesystem::path& path, bool recursive) {
    if (running_) {
        throw std::runtime_error("Cannot add watch while watcher is running");
    }
    if (!std::filesystem::exists(path)) {
        throw std::runtime_error("Path does not exist: " + path.string());
    }
    
    LOG_INFO("FileWatcher: Adding watch for path: " + path.string() + " (recursive: " + (recursive ? "true" : "false") + ")");
    pimpl_->AddWatchImpl(path, recursive);
}

void FileWatcher::RemoveWatch(const std::filesystem::path& path) {
    if (running_) {
        throw std::runtime_error("Cannot remove watch while watcher is running");
    }

    LOG_INFO("FileWatcher: Removing watch for path: " + path.string());
    pimpl_->RemoveWatchImpl(path);
}

void FileWatcher::SetEventCallback(FileEventCallback callback) {
    callback_ = std::move(callback);
}

void FileWatcher::Start() {
    if (running_) {
        LOG_DEBUG("FileWatcher: Already running, ignoring Start()");
        return;
    }
    
    if (!callback_) {
        throw std::runtime_error("Event callback must be set before starting");
    }
    
    LOG_INFO("FileWatcher: Starting file watcher");
    running_ = true;
    pimpl_->StartImpl();
    LOG_INFO("FileWatcher: File watcher started successfully");
}

void FileWatcher::Stop() {
    if (!running_) {
        LOG_DEBUG("FileWatcher: Not running, ignoring Stop()");
        return;
    }
    
    LOG_INFO("FileWatcher: Stopping file watcher");
    running_ = false;
    pimpl_->StopImpl();
    LOG_INFO("FileWatcher: File watcher stopped");
}

bool FileWatcher::IsRunning() const {
    return running_;
}

FileEventCallback& FileWatcher::Impl::GetCallback() {
    return owner_->callback_;
}

bool FileWatcher::Impl::IsOwnerRunning() const {
    return owner_->running_;
}

}  // namespace synxpo
