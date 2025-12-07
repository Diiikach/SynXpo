#include "synxpo/client/file_watcher.h"

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
        
    pimpl_->AddWatchImpl(path, recursive);
}

void FileWatcher::RemoveWatch(const std::filesystem::path& path) {
    if (running_) {
        throw std::runtime_error("Cannot remove watch while watcher is running");
    }

    pimpl_->RemoveWatchImpl(path);
}

void FileWatcher::SetEventCallback(FileEventCallback callback) {
    callback_ = std::move(callback);
}

void FileWatcher::Start() {
    if (running_) {
        return;
    }
    
    if (!callback_) {
        throw std::runtime_error("Event callback must be set before starting");
    }
    
    running_ = true;
    pimpl_->StartImpl();
}

void FileWatcher::Stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    pimpl_->StopImpl();
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
