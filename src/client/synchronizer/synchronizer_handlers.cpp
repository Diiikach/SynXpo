#include "synxpo/client/synchronizer.h"
#include "synxpo/client/logger.h"

namespace synxpo {

void Synchronizer::OnServerMessage(const ServerMessage& message) {
    std::string msg_type = "UNKNOWN";
    switch (message.message_case()) {
        case ServerMessage::kOkDirectoryCreated: msg_type = "OK_DIRECTORY_CREATED"; break;
        case ServerMessage::kOkSubscribed: msg_type = "OK_SUBSCRIBED"; break;
        case ServerMessage::kVersionIncreaseAllow: msg_type = "VERSION_INCREASE_ALLOW"; break;
        case ServerMessage::kVersionIncreaseDeny: msg_type = "VERSION_INCREASE_DENY"; break;
        case ServerMessage::kVersionIncreased: msg_type = "VERSION_INCREASED"; break;
        case ServerMessage::kCheckVersion: msg_type = "CHECK_VERSION"; break;
        case ServerMessage::kFileContentRequestAllow: msg_type = "FILE_CONTENT_REQUEST_ALLOW"; break;
        case ServerMessage::kFileContentRequestDeny: msg_type = "FILE_CONTENT_REQUEST_DENY"; break;
        case ServerMessage::kFileWrite: msg_type = "FILE_WRITE"; break;
        case ServerMessage::kFileWriteEnd: msg_type = "FILE_WRITE_END"; break;
        case ServerMessage::kError: msg_type = "ERROR"; break;
        default: break;
    }
    LOG_DEBUG("[RECV] " + msg_type);
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
    
    auto status = ProcessCheckVersion(directory_id, server_files);
    if (!status.ok()) {
        // TODO: Log error
    }
}

void Synchronizer::HandleFileContentRequestAllow(const FileContentRequestAllow& msg) {
    // Response is now handled synchronously in RequestFileContents
    // Just prepare for receiving FILE_WRITE messages
    std::lock_guard<std::mutex> lock(transfer_mutex_);
    
    download_state_.active = true;
    download_state_.last_activity = std::chrono::system_clock::now();
}

void Synchronizer::HandleFileContentRequestDeny(const FileContentRequestDeny& msg) {
    // This handler is kept for compatibility but shouldn't be called
    // when using SendMessageWithResponse
}

void Synchronizer::HandleFileWrite(const FileWrite& msg) {
    std::lock_guard<std::mutex> lock(transfer_mutex_);
    
    if (!download_state_.active) {
        return;
    }
    
    download_state_.last_activity = std::chrono::system_clock::now();
    
    const auto& chunk = msg.chunk();
    const std::string& file_id = chunk.id();
    const std::string& directory_id = chunk.directory_id();
    
    // Open stream if not already open
    if (download_state_.write_streams.find(file_id) == download_state_.write_streams.end()) {
        auto file_meta_result = storage_.GetFileMetadata(directory_id, file_id);
        if (!file_meta_result.ok()) {
            // TODO: Handle error
            return;
        }
        
        const auto& file_meta = *file_meta_result;
        auto dir_files = storage_.ListDirectoryFiles(directory_id);
        if (!dir_files.ok()) {
            return;
        }
        
        auto dir_path = GetDirectoryPath(directory_id);
        if (!dir_path.has_value()) {
            return;
        }
        
        auto file_path = *dir_path / file_meta.current_path();
        auto temp_path = file_path;
        temp_path += ".synxpo_tmp";
        
        if (std::filesystem::exists(file_path)) {
            auto backup_status = BackupFile(file_path);
            (void)backup_status;  // Ignore backup errors for now
        }
        
        std::filesystem::create_directories(temp_path.parent_path());
        
        download_state_.write_streams[file_id].open(temp_path, std::ios::binary);
        download_state_.temp_paths[file_id] = temp_path;
        
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
        if (temp_it != download_state_.temp_paths.end()) {
            auto temp_path = temp_it->second;
            auto final_path = temp_path;
            final_path.replace_extension("");
            final_path = final_path.parent_path() / final_path.stem();
            
            std::filesystem::rename(temp_path, final_path);
            written_files.push_back(final_path);
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
    download_state_.active = false;
}

void Synchronizer::HandleError(const Error& msg) {
    // TODO: Handle server errors
}

}  // namespace synxpo
