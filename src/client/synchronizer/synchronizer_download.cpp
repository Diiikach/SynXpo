#include "synxpo/client/synchronizer.h"

#include <thread>
#include <unordered_map>

namespace synxpo {

absl::Status Synchronizer::RequestVersions(const std::string& directory_id) {
    ClientMessage msg;
    auto* request = msg.mutable_request_version();
    auto* file_request = request->add_requests();
    file_request->set_directory_id(directory_id);

    return grpc_client_.SendMessage(msg);
}

absl::Status Synchronizer::RequestFileVersions(
    const std::string& directory_id,
    const std::vector<std::string>& file_ids) {
    
    ClientMessage msg;
    auto* request = msg.mutable_request_version();

    for (const auto& file_id : file_ids) {
        auto* file_request = request->add_requests();
        auto* file_id_msg = file_request->mutable_file_id();
        file_id_msg->set_id(file_id);
        file_id_msg->set_directory_id(directory_id);
    }

    return grpc_client_.SendMessage(msg);
}

absl::Status Synchronizer::ProcessCheckVersion(
    const std::string& directory_id,
    const std::vector<FileMetadata>& server_files) {
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        directory_states_[directory_id].is_syncing = true;
    }
    
    auto diff = CalculateVersionDiff(directory_id, server_files);
    
    // Apply renames and deletes first
    if (!diff.to_rename_delete.empty()) {
        auto status = ApplyRenamesAndDeletes(directory_id, diff.to_rename_delete);
        if (!status.ok()) {
            return status;
        }
    }
    
    // Request file contents that need updating
    if (!diff.to_download.empty()) {
        auto status = RequestFileContents(directory_id, diff.to_download);
        if (!status.ok()) {
            return status;
        }
        // Files will be received via FILE_WRITE messages in handlers
    }
    
    // Delete files that don't exist on server
    if (!diff.to_delete_local.empty()) {
        auto status = DeleteMissingFiles(directory_id, diff.to_delete_local);
        if (!status.ok()) {
            return status;
        }
    }
    
    // Upload files that are newer locally
    if (!diff.to_upload.empty()) {
        std::vector<FileChangeInfo> changes;
        for (const auto& file_meta : diff.to_upload) {
            FileChangeInfo info;
            info.file_id = file_meta.id();
            info.directory_id = directory_id;
            info.current_path = file_meta.current_path();
            info.deleted = file_meta.deleted();
            info.content_changed = true;
            info.first_try_time = std::chrono::system_clock::now();
            changes.push_back(info);
        }
        
        auto status = AskVersionIncrease(directory_id, changes);
        if (!status.ok()) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            directory_states_[directory_id].is_syncing = false;
            return status;
        }
    }
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        directory_states_[directory_id].is_syncing = false;
    }
    
    return absl::OkStatus();
}

Synchronizer::VersionDiff Synchronizer::CalculateVersionDiff(
    const std::string& directory_id,
    const std::vector<FileMetadata>& server_files) {
    
    VersionDiff diff;
    
    // Get local files
    auto local_files_result = storage_.ListDirectoryFiles(directory_id);
    if (!local_files_result.ok()) {
        return diff;
    }
    
    auto local_files = *local_files_result;
    std::unordered_map<std::string, FileMetadata> local_map;
    for (const auto& file : local_files) {
        local_map[file.id()] = file;
    }
    
    // Compare server files with local
    for (const auto& server_file : server_files) {
        auto local_it = local_map.find(server_file.id());
        
        if (local_it == local_map.end()) {
            // File doesn't exist locally - download it
            if (server_file.content_changed_version() > 0) {
                diff.to_download.push_back(server_file);
            }
            diff.to_rename_delete.push_back(server_file);
        } else {
            const auto& local_file = local_it->second;
            
            // Check if content needs updating
            if (server_file.content_changed_version() > local_file.content_changed_version()) {
                diff.to_download.push_back(server_file);
            }
            
            // Check if path or deleted status changed
            if (server_file.current_path() != local_file.current_path() ||
                server_file.deleted() != local_file.deleted()) {
                diff.to_rename_delete.push_back(server_file);
            }
            
            // Check if local version is newer (shouldn't happen normally)
            if (local_file.version() > server_file.version()) {
                diff.to_upload.push_back(local_file);
            }
            
            // Remove from local_map to track which files to delete
            local_map.erase(local_it);
        }
    }
    
    // Remaining files in local_map don't exist on server
    for (const auto& [file_id, file_meta] : local_map) {
        if (file_meta.version() > 0) {
            diff.to_delete_local.push_back(file_id);
        } else {
            diff.to_upload.push_back(file_meta);
        }
    }
    
    return diff;
}

absl::Status Synchronizer::ApplyRenamesAndDeletes(
    const std::string& directory_id,
    const std::vector<FileMetadata>& files) {
    
    auto dir_path = GetDirectoryPath(directory_id);
    if (!dir_path.has_value()) {
        return absl::NotFoundError("Directory not found in config");
    }
    
    // Сollect all affected file paths
    std::vector<std::filesystem::path> affected_files;
    
    for (const auto& file_meta : files) {
        auto file_path = *dir_path / file_meta.current_path();
        
        if (file_meta.deleted()) {
            if (std::filesystem::exists(file_path)) {
                affected_files.push_back(file_path);
            }
        } else {
            auto local_meta_result = storage_.GetFileMetadata(directory_id, file_meta.id());
            if (local_meta_result.ok()) {
                auto old_path = *dir_path / local_meta_result->current_path();
                if (old_path != file_path && std::filesystem::exists(old_path)) {
                    affected_files.push_back(old_path);
                    affected_files.push_back(file_path);
                }
            }
        }
    }
    
    // Block files before making any changes
    if (!affected_files.empty()) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& dir_state = directory_states_[directory_id];
        for (const auto& file_path : affected_files) {
            dir_state.files_being_written.insert(file_path);
        }
    }
    
    // Perform renames and deletes
    for (const auto& file_meta : files) {
        auto file_path = *dir_path / file_meta.current_path();
        
        if (file_meta.deleted()) {
            if (std::filesystem::exists(file_path)) {
                auto backup_status = BackupFile(file_path);
                (void)backup_status;  // Ignore backup errors
                std::filesystem::remove(file_path);
            }
        } else {
            auto local_meta_result = storage_.GetFileMetadata(directory_id, file_meta.id());
            if (local_meta_result.ok()) {
                auto old_path = *dir_path / local_meta_result->current_path();
                if (old_path != file_path && std::filesystem::exists(old_path)) {
                    auto backup_status = BackupFile(old_path);
                    (void)backup_status;  // Ignore backup errors
                    std::filesystem::create_directories(file_path.parent_path());
                    std::filesystem::rename(old_path, file_path);
                }
            }
        }
        
        auto upsert_status = storage_.UpsertFile(file_meta);
        (void)upsert_status;  // Continue even if upsert fails
    }
    
    // Wait for filesystem to settle, then unblock
    if (!affected_files.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& dir_state = directory_states_[directory_id];
        for (const auto& file_path : affected_files) {
            dir_state.files_being_written.erase(file_path);
        }
    }
    
    return absl::OkStatus();
}

absl::Status Synchronizer::RequestFileContents(
    const std::string& directory_id,
    const std::vector<FileMetadata>& files) {
    
    ClientMessage msg;
    auto* request = msg.mutable_request_file_content();

    for (const auto& file : files) {
        auto* file_id = request->add_files();
        file_id->set_id(file.id());
        file_id->set_directory_id(file.directory_id());
    }

    auto response = grpc_client_.SendMessageWithResponse(msg);
    if (!response.ok()) {
        return response.status();
    }
    
    if (response->has_file_content_request_allow()) {
        // Prepare for receiving FILE_WRITE messages
        {
            std::lock_guard<std::mutex> lock(transfer_mutex_);
            download_state_.directory_id = directory_id;
            download_state_.files = files;
            download_state_.active = true;
            download_state_.last_activity = std::chrono::system_clock::now();
        }
        
        // Wait for FILE_WRITE_END
        // Note: FILE_WRITE messages are handled asynchronously by HandleFileWrite
        // We need to wait until download_state_.active becomes false
        auto timeout = std::chrono::seconds(60);  // 60 second timeout
        auto start = std::chrono::steady_clock::now();
        
        while (true) {
            {
                std::lock_guard<std::mutex> lock(transfer_mutex_);
                if (!download_state_.active) {
                    break;
                }
            }
            
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > timeout) {
                std::lock_guard<std::mutex> lock(transfer_mutex_);
                download_state_.active = false;
                download_state_.write_streams.clear();
                download_state_.temp_paths.clear();
                download_state_.final_paths.clear();
                return absl::DeadlineExceededError("Timeout waiting for file download");
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        return absl::OkStatus();
    }
    
    if (response->has_file_content_request_deny()) {
        // Handle denied files
        std::vector<FileStatusInfo> file_statuses(
            response->file_content_request_deny().files().begin(),
            response->file_content_request_deny().files().end());
        return HandleFileContentRequestDeny(directory_id, file_statuses);
    }
    
    return absl::InternalError("Unexpected response type for REQUEST_FILE_CONTENT");
}

absl::Status Synchronizer::ReceiveAndWriteFiles(
    const std::string& directory_id,
    const std::vector<FileMetadata>& files) {
    
    // TODO: Implement file download and writing
    return absl::UnimplementedError("ReceiveAndWriteFiles not implemented");
}

absl::Status Synchronizer::HandleFileContentRequestDeny(
    const std::string& directory_id,
    const std::vector<FileStatusInfo>& file_statuses) {
    
    std::vector<FileMetadata> free_files;
    
    // Get local file metadata for FREE files to retry
    for (const auto& status_info : file_statuses) {
        if (status_info.status() == FileStatus::FREE) {
            auto file_meta_result = storage_.GetFileMetadata(
                directory_id, status_info.id());
            if (file_meta_result.ok()) {
                free_files.push_back(*file_meta_result);
            }
        }
        // For BLOCKED files, do nothing - they will trigger CHECK_VERSION when ready
    }
    
    // Retry FREE files immediately
    if (!free_files.empty()) {
        return RequestFileContents(directory_id, free_files);
    }
    
    return absl::OkStatus();
}

absl::Status Synchronizer::DeleteMissingFiles(
    const std::string& directory_id,
    const std::vector<std::string>& file_ids) {
    
    auto dir_path = GetDirectoryPath(directory_id);
    if (!dir_path.has_value()) {
        return absl::NotFoundError("Directory not found in config");
    }
    
    for (const auto& file_id : file_ids) {
        auto file_meta_result = storage_.GetFileMetadata(directory_id, file_id);
        if (!file_meta_result.ok()) {
            continue;
        }
        
        auto file_path = *dir_path / file_meta_result->current_path();
        if (std::filesystem::exists(file_path)) {
            auto backup_status = BackupFile(file_path);
            (void)backup_status;  // Ignore backup errors
            std::filesystem::remove(file_path);
        }
        
        auto remove_status = storage_.RemoveFile(directory_id, file_id);
        (void)remove_status;  // Continue even if removal fails
    }
    
    return absl::OkStatus();
}

}  // namespace synxpo
