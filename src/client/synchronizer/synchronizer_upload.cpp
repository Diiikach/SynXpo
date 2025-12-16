#include "synxpo/client/synchronizer.h"

#include <fstream>

#include <absl/log/log.h>

namespace synxpo {

void Synchronizer::OnFileEvent(const FileEvent& event) {
    LOG(INFO) << "[Client] OnFileEvent: path=" << event.path 
              << " type=" << static_cast<int>(event.type);
    
    auto dir_id_opt = storage_.GetDirectoryIdByPath(event.path);
    if (!dir_id_opt.has_value()) {
        LOG(WARNING) << "[Client] OnFileEvent: no directory for path " << event.path;
        return;
    }

    const std::string& directory_id = *dir_id_opt;
    
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto& dir_state = directory_states_[directory_id];
    
    // Ignore events for files we're currently writing (prevents sync loops)
    if (dir_state.files_being_written.count(event.path) > 0) {
        LOG(INFO) << "[Client] OnFileEvent: ignoring (file being written)";
        return;
    }
    
    auto change_info = EventToChangeInfo(event);
    LOG(INFO) << "[Client] OnFileEvent: change_info.deleted=" << change_info.deleted 
              << " content_changed=" << change_info.content_changed;
    dir_state.pending_changes[event.path] = change_info;
    dir_state.last_change_time = std::chrono::system_clock::now();
}

absl::Status Synchronizer::AskVersionIncrease(
    const std::string& directory_id,
    const std::vector<FileChangeInfo>& changes) {
    
    ClientMessage msg;
    auto* ask = msg.mutable_ask_version_increase();

    LOG(INFO) << "[Sync] AskVersionIncrease for " << changes.size() << " files:";
    for (const auto& change : changes) {
        auto* file_info = ask->add_files();
        
        if (change.file_id.has_value()) {
            file_info->set_id(*change.file_id);
        }
        file_info->set_directory_id(directory_id);
        file_info->set_current_path(change.current_path.string());
        file_info->set_deleted(change.deleted);
        file_info->set_content_changed(change.content_changed);
        
        LOG(INFO) << "[Sync]   - path=" << change.current_path 
                  << " deleted=" << change.deleted 
                  << " content_changed=" << change.content_changed
                  << " file_id=" << (change.file_id.has_value() ? *change.file_id : "(new)");
        
        auto* timestamp = file_info->mutable_first_try_time();
        auto time_since_epoch = change.first_try_time.time_since_epoch();
        auto micros = std::chrono::duration_cast<std::chrono::microseconds>(time_since_epoch);
        timestamp->set_time(micros.count());
    }

    auto response = grpc_client_.SendMessageWithResponse(msg);
    if (!response.ok()) {
        return response.status();
    }
    
    if (response->has_version_increase_allow()) {
        std::vector<FileChangeInfo> files_with_content;
        for (const auto& change : changes) {
            if (change.content_changed) {
                files_with_content.push_back(change);
            }
        }
        
        if (!files_with_content.empty()) {
            return UploadFileContents(directory_id, files_with_content);
        }
        return absl::OkStatus();
    }
    
    if (response->has_version_increase_deny()) {
        std::vector<FileStatusInfo> file_statuses(
            response->version_increase_deny().files().begin(),
            response->version_increase_deny().files().end());
        return HandleVersionIncreaseDeny(directory_id, file_statuses);
    }
    
    if (response->has_version_increased()) {
        // Update metadata for files that were changed without content upload
        for (const auto& file_meta : response->version_increased().files()) {
            auto status = storage_.UpsertFile(file_meta);
            if (!status.ok()) {
                // Log error but continue
            }
        }
        return absl::OkStatus();
    }
    
    return absl::InternalError("Unexpected response type for ASK_VERSION_INCREASE");
}

absl::Status Synchronizer::UploadFileContents(
    const std::string& directory_id,
    const std::vector<FileChangeInfo>& files) {
    
    auto dir_path = GetDirectoryPath(directory_id);
    if (!dir_path.has_value()) {
        return absl::NotFoundError("Directory not found in config");
    }
    
    const size_t chunk_size = config_.GetChunkSize();
    
    for (const auto& file_info : files) {
        if (file_info.deleted || !file_info.content_changed) {
            continue;
        }
        
        auto file_path = *dir_path / file_info.current_path;
        
        if (!std::filesystem::exists(file_path)) {
            // File was deleted after we queued it
            continue;
        }
        
        std::ifstream file(file_path, std::ios::binary);
        if (!file) {
            return absl::InternalError("Failed to open file: " + file_path.string());
        }
        
        std::vector<char> buffer(chunk_size);
        uint64_t offset = 0;
        
        while (file.read(buffer.data(), chunk_size) || file.gcount() > 0) {
            ClientMessage msg;
            auto* write = msg.mutable_file_write();
            auto* chunk = write->mutable_chunk();
            
            if (file_info.file_id.has_value()) {
                chunk->set_id(*file_info.file_id);
            }
            chunk->set_directory_id(directory_id);
            chunk->set_offset(offset);
            chunk->set_current_path(file_info.current_path.string());
            chunk->set_data(buffer.data(), file.gcount());
            
            auto status = grpc_client_.SendMessage(msg);
            if (!status.ok()) {
                return status;
            }
            
            offset += file.gcount();
        }
    }
    
    // Send FILE_WRITE_END and wait for VERSION_INCREASED
    ClientMessage msg;
    msg.mutable_file_write_end();
    
    auto response = grpc_client_.SendMessageWithResponse(msg);
    if (!response.ok()) {
        return response.status();
    }
    
    if (response->has_version_increased()) {
        // Update local metadata with server response
        for (const auto& file_meta : response->version_increased().files()) {
            auto status = storage_.UpsertFile(file_meta);
            if (!status.ok()) {
                // Log error but continue
            }
        }
        return absl::OkStatus();
    }
    
    if (response->has_error()) {
        return absl::InternalError(response->error().message());
    }
    
    return absl::InternalError("Unexpected response after FILE_WRITE_END");
}

absl::Status Synchronizer::HandleVersionIncreaseDeny(
    const std::string& directory_id,
    const std::vector<FileStatusInfo>& file_statuses) {
    
    std::vector<FileChangeInfo> free_files;
    std::vector<std::string> denied_file_ids;
    
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto& dir_state = directory_states_[directory_id];
    
    for (const auto& status_info : file_statuses) {
        const std::string& file_id = status_info.id();
        
        switch (status_info.status()) {
            case FileStatus::FREE: {
                for (auto& [path, change_info] : dir_state.pending_changes) {
                    if (change_info.file_id == file_id) {
                        free_files.push_back(change_info);
                        break;
                    }
                }
                break;
            }
            
            case FileStatus::BLOCKED: {
                dir_state.blocked_files.insert(file_id);
                break;
            }
            
            case FileStatus::DENIED: {
                denied_file_ids.push_back(file_id);
                
                for (auto it = dir_state.pending_changes.begin(); 
                     it != dir_state.pending_changes.end();) {
                    if (it->second.file_id == file_id) {
                        it = dir_state.pending_changes.erase(it);
                    } else {
                        ++it;
                    }
                }
                break;
            }
        }
    }
    
    if (!free_files.empty()) {
        auto status = AskVersionIncrease(directory_id, free_files);
        if (!status.ok()) {
            return status;
        }
    }
    
    if (!denied_file_ids.empty()) {
        auto status = RequestFileVersions(directory_id, denied_file_ids);
        if (!status.ok()) {
            return status;
        }
    }
    
    return absl::OkStatus();
}

void Synchronizer::ProcessPendingChanges(const std::string& directory_id) {
    std::vector<FileChangeInfo> changes_to_send;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = directory_states_.find(directory_id);
        if (it == directory_states_.end() || it->second.pending_changes.empty()) {
            return;
        }
        
        for (const auto& [path, change_info] : it->second.pending_changes) {
            changes_to_send.push_back(change_info);
        }
        
        it->second.pending_changes.clear();
    }
    
    if (!changes_to_send.empty()) {
        auto status = AskVersionIncrease(directory_id, changes_to_send);
        if (!status.ok()) {
            // TODO: Log error, changes will be retried on next CHECK_VERSION
        }
    }
}

}  // namespace synxpo
