#include "synxpo/client/synchronizer.h"

#include <absl/log/log.h>

namespace synxpo {

void Synchronizer::OnServerMessage(const ServerMessage& message) {
    switch (message.message_case()) {
        case ServerMessage::kOkDirectoryCreated:
            HandleOkDirectoryCreated(message.ok_directory_created());
            break;
        case ServerMessage::kOkSubscribed:
            HandleOkSubscribed(message.ok_subscribed());
            break;
        case ServerMessage::kVersionIncreaseAllow:
            HandleVersionIncreaseAllow(message.version_increase_allow());
            break;
        case ServerMessage::kVersionIncreaseDeny:
            HandleVersionIncreaseDeny(message.version_increase_deny());
            break;
        case ServerMessage::kVersionIncreased:
            HandleVersionIncreased(message.version_increased());
            break;
        case ServerMessage::kCheckVersion:
            HandleCheckVersion(message.check_version());
            break;
        case ServerMessage::kFileContentRequestAllow:
            HandleFileContentRequestAllow(message.file_content_request_allow());
            break;
        case ServerMessage::kFileContentRequestDeny:
            HandleFileContentRequestDeny(message.file_content_request_deny());
            break;
        case ServerMessage::kFileWrite:
            HandleFileWrite(message.file_write());
            break;
        case ServerMessage::kFileWriteEnd:
            HandleFileWriteEnd(message.file_write_end());
            break;
        case ServerMessage::kError:
            HandleError(message.error());
            break;
        default:
            break;
    }
}

void Synchronizer::HandleOkDirectoryCreated(const OkDirectoryCreated& msg) {
    // This handler is kept for compatibility but shouldn't be called
    // when using SendMessageWithResponse
}

void Synchronizer::HandleOkSubscribed(const OkSubscribed& msg) {
    // This handler is kept for compatibility but shouldn't be called
    // when using SendMessageWithResponse
}

void Synchronizer::HandleVersionIncreaseAllow(const VersionIncreaseAllow& msg) {
    // This handler is kept for compatibility but shouldn't be called
    // when using SendMessageWithResponse
}

void Synchronizer::HandleVersionIncreaseDeny(const VersionIncreaseDeny& msg) {
    // This handler is kept for compatibility but shouldn't be called
    // when using SendMessageWithResponse
}

void Synchronizer::HandleVersionIncreased(const VersionIncreased& msg) {
    for (const auto& file_meta : msg.files()) {
        auto status = storage_.UpsertFile(file_meta);
        if (!status.ok()) {
            // TODO: Log error
            continue;
        }
        
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = directory_states_.find(file_meta.directory_id());
        if (it != directory_states_.end()) {
            // Remove from pending changes
            for (auto path_it = it->second.pending_changes.begin(); 
                 path_it != it->second.pending_changes.end();) {
                if (path_it->second.file_id == file_meta.id() ||
                    path_it->second.current_path == file_meta.current_path()) {
                    path_it = it->second.pending_changes.erase(path_it);
                } else {
                    ++path_it;
                }
            }
        }
    }
}

void Synchronizer::HandleCheckVersion(const CheckVersion& msg) {
    std::vector<FileMetadata> server_files(msg.files().begin(), msg.files().end());
    
    if (server_files.empty()) {
        return;
    }
    
    const std::string& directory_id = server_files[0].directory_id();
    
    // Process in a separate thread to avoid blocking the callback worker
    // The callback worker needs to be free to process FILE_WRITE messages
    std::thread([this, directory_id, server_files = std::move(server_files)]() {
        auto status = ProcessCheckVersion(directory_id, server_files);
        if (!status.ok()) {
            LOG(ERROR) << "[Sync] ProcessCheckVersion failed: " << status.message();
        }
    }).detach();
}

void Synchronizer::HandleFileContentRequestAllow(const FileContentRequestAllow& msg) {
    // Response is now handled synchronously in RequestFileContents
    // download_state_.active is set before sending the request to avoid race conditions
    // This handler is kept for compatibility but the state is already set
}

void Synchronizer::HandleFileContentRequestDeny(const FileContentRequestDeny& msg) {
    // This handler is kept for compatibility but shouldn't be called
    // when using SendMessageWithResponse
}

void Synchronizer::HandleFileWrite(const FileWrite& msg) {
    std::lock_guard<std::mutex> lock(transfer_mutex_);
    
    if (!download_state_.active) {
        LOG(WARNING) << "[Sync] HandleFileWrite: download_state_ not active, ignoring";
        return;
    }
    
    download_state_.last_activity = std::chrono::system_clock::now();
    
    const auto& chunk = msg.chunk();
    const std::string& file_id = chunk.id();
    const std::string& directory_id = chunk.directory_id().empty() 
        ? download_state_.directory_id 
        : chunk.directory_id();
    
    LOG(INFO) << "[Sync] HandleFileWrite: file_id=" << file_id 
              << " directory_id=" << directory_id
              << " offset=" << chunk.offset() 
              << " size=" << chunk.data().size();
    
    // Open stream if not already open
    if (download_state_.write_streams.find(file_id) == download_state_.write_streams.end()) {
        // First try to get path from chunk (preferred for new downloads)
        std::string current_path = chunk.current_path();
        
        LOG(INFO) << "[Sync] HandleFileWrite: current_path from chunk='" << current_path << "'";
        
        // If no path in chunk, try to get from local metadata
        if (current_path.empty()) {
            auto file_meta_result = storage_.GetFileMetadata(directory_id, file_id);
            if (file_meta_result.ok()) {
                current_path = file_meta_result->current_path();
                LOG(INFO) << "[Sync] HandleFileWrite: current_path from metadata='" << current_path << "'";
            } else {
                LOG(WARNING) << "[Sync] HandleFileWrite: failed to get metadata: " << file_meta_result.status().message();
            }
        }
        
        if (current_path.empty()) {
            // Cannot determine file path, skip this chunk
            LOG(ERROR) << "[Sync] HandleFileWrite: cannot determine file path, skipping";
            return;
        }
        
        auto dir_path = GetDirectoryPath(directory_id);
        if (!dir_path.has_value()) {
            LOG(ERROR) << "[Sync] HandleFileWrite: cannot get directory path for " << directory_id;
            return;
        }
        
        auto file_path = *dir_path / current_path;
        auto temp_path = file_path;
        temp_path += ".synxpo_tmp";
        
        LOG(INFO) << "[Sync] HandleFileWrite: writing to temp_path=" << temp_path;
        
        if (std::filesystem::exists(file_path)) {
            auto backup_status = BackupFile(file_path);
            (void)backup_status;  // Ignore backup errors for now
        }
        
        std::filesystem::create_directories(temp_path.parent_path());
        
        download_state_.write_streams[file_id].open(temp_path, std::ios::binary);
        download_state_.temp_paths[file_id] = temp_path;
        download_state_.final_paths[file_id] = file_path;
        
        // Mark file as being written to ignore FileWatcher events
        {
            std::lock_guard<std::mutex> state_lock(state_mutex_);
            directory_states_[directory_id].files_being_written.insert(file_path);
        }
    }
    
    auto& stream = download_state_.write_streams[file_id];
    stream.write(chunk.data().data(), chunk.data().size());
}

void Synchronizer::HandleFileWriteEnd(const FileWriteEnd& msg) {
    std::lock_guard<std::mutex> lock(transfer_mutex_);
    
    if (!download_state_.active) {
        return;
    }
    
    std::vector<std::filesystem::path> written_files;
    
    // Close all streams and move temp files to final locations
    for (auto& [file_id, stream] : download_state_.write_streams) {
        stream.close();
        
        auto temp_it = download_state_.temp_paths.find(file_id);
        auto final_it = download_state_.final_paths.find(file_id);
        
        if (temp_it != download_state_.temp_paths.end() && 
            final_it != download_state_.final_paths.end()) {
            auto& temp_path = temp_it->second;
            auto& final_path = final_it->second;
            
            std::error_code ec;
            std::filesystem::rename(temp_path, final_path, ec);
            if (!ec) {
                written_files.push_back(final_path);
            }
        }
    }
    
    // Clear files_being_written for all written files
    if (!written_files.empty() && !download_state_.directory_id.empty()) {
        std::lock_guard<std::mutex> state_lock(state_mutex_);
        auto& dir_state = directory_states_[download_state_.directory_id];
        for (const auto& file_path : written_files) {
            dir_state.files_being_written.erase(file_path);
        }
    }
    
    download_state_.write_streams.clear();
    download_state_.temp_paths.clear();
    download_state_.final_paths.clear();
    download_state_.active = false;
}

void Synchronizer::HandleError(const Error& msg) {
    // TODO: Handle server errors
}

}  // namespace synxpo
