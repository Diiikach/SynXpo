#include "synxpo/client/synchronizer.h"

#include <algorithm>

namespace synxpo {

Synchronizer::Synchronizer(
    ClientConfig& config,
    IFileMetadataStorage& storage,
    GRPCClient& grpc_client,
    FileWatcher& file_watcher)
    : config_(config),
      storage_(storage),
      grpc_client_(grpc_client),
      file_watcher_(file_watcher) {
}

Synchronizer::~Synchronizer() {
    if (auto_sync_running_) {
        StopAutoSync();
    }
}

absl::Status Synchronizer::StartAutoSync() {
    if (auto_sync_running_) {
        return absl::AlreadyExistsError("Auto sync is already running");
    }

    auto status = InitializeDirectories();
    if (!status.ok()) {
        return status;
    }

    // Set up file watcher for all directories
    file_watcher_.SetEventCallback(
        [this](const FileEvent& event) { OnFileEvent(event); });
    
    for (const auto& dir : config_.GetDirectories()) {
        if (dir.enabled && !dir.local_path.empty()) {
            file_watcher_.AddWatch(dir.local_path, true);
        }
    }
    
    file_watcher_.Start();
    
    grpc_client_.SetMessageCallback(
        [this](const ServerMessage& message) { OnServerMessage(message); });

    debounce_thread_running_.store(true);
    debounce_thread_ = std::thread([this]() {
        auto debounce_duration = config_.GetWatchDebounce();
        
        while (debounce_thread_running_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            auto now = std::chrono::system_clock::now();
            std::vector<std::string> directories_to_process;
            
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                for (auto& [dir_id, dir_state] : directory_states_) {
                    if (!dir_state.pending_changes.empty()) {
                        auto time_since_change = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - dir_state.last_change_time);
                        
                        if (time_since_change >= debounce_duration) {
                            directories_to_process.push_back(dir_id);
                        }
                    }
                }
            }
            
            for (const auto& dir_id : directories_to_process) {
                ProcessPendingChanges(dir_id);
            }
        }
    });

    auto_sync_running_ = true;
    return absl::OkStatus();
}

void Synchronizer::StopAutoSync() {
    if (!auto_sync_running_) {
        return;
    }

    auto_sync_running_ = false;

    debounce_thread_running_.store(false);
    if (debounce_thread_.joinable()) {
        debounce_thread_.join();
    }

    file_watcher_.Stop();
    file_watcher_.SetEventCallback(nullptr);
    grpc_client_.SetMessageCallback(nullptr);
}

bool Synchronizer::IsAutoSyncRunning() const {
    return auto_sync_running_;
}

absl::Status Synchronizer::SyncOnce() {
    std::lock_guard<std::mutex> lock(sync_mutex_);

    auto directories = config_.GetDirectories();
    for (const auto& dir : directories) {
        if (!dir.directory_id.empty()) {
            auto status = SyncDirectory(dir.directory_id);
            if (!status.ok()) {
                return status;
            }
        }
    }

    return absl::OkStatus();
}

absl::Status Synchronizer::SyncDirectory(const std::string& directory_id) {
    std::lock_guard<std::mutex> lock(sync_mutex_);

    auto status = RequestVersions(directory_id);
    if (!status.ok()) {
        return status;
    }

    // CHECK_VERSION response will be processed asynchronously in HandleCheckVersion

    return absl::OkStatus();
}


absl::Status Synchronizer::BackupFile(const std::filesystem::path& file_path) {
    // TODO: Implement backup
    return absl::OkStatus();
}

absl::Status Synchronizer::RestoreFile(const std::filesystem::path& file_path) {
    // TODO: Implement restore
    return absl::OkStatus();
}

void Synchronizer::CleanupBackups() {
    // TODO: Implement backup cleanup
}

Synchronizer::FileChangeInfo Synchronizer::EventToChangeInfo(const FileEvent& event) {
    FileChangeInfo info;
    
    // Get directory_id
    auto dir_id = storage_.GetDirectoryIdByPath(event.path);
    if (!dir_id.has_value()) {
        return info;  // Should not happen - caller checks this
    }
    
    info.directory_id = *dir_id;
    
    // Get the directory's local path to compute relative path
    auto dir_path = GetDirectoryPath(*dir_id);
    if (!dir_path.has_value()) {
        return info;  // Should not happen
    }
    
    // Convert absolute path to relative path
    info.current_path = std::filesystem::relative(event.path, *dir_path);
    
    // Try to get file_id from storage if file exists
    auto file_meta_result = storage_.GetFileMetadata(info.current_path, *dir_id);
    if (file_meta_result.ok()) {
        info.file_id = file_meta_result->id();
    } else {
        info.file_id = std::nullopt;  // New file
    }
    
    info.deleted = (event.type == FileEventType::Deleted);
    info.content_changed = (event.type == FileEventType::Modified || 
                           event.type == FileEventType::Created);
    info.first_try_time = event.timestamp;
    
    return info;
}

std::optional<std::filesystem::path> Synchronizer::GetDirectoryPath(
    const std::string& directory_id) const {
    for (const auto& dir_config : config_.GetDirectories()) {
        if (dir_config.directory_id == directory_id) {
            return dir_config.local_path;
        }
    }
    return std::nullopt;
}

}  // namespace synxpo
