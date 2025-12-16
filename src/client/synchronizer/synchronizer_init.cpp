#include "synxpo/client/synchronizer.h"

namespace synxpo {

absl::Status Synchronizer::InitializeDirectories() {
    auto config_directories = config_.GetDirectories();
    auto storage_directories = storage_.ListDirectories();

    std::set<std::string> storage_dir_ids(storage_directories.begin(), 
                                          storage_directories.end());
    
    std::set<std::string> config_dir_ids;
    absl::Status first_error = absl::OkStatus();
    
    for (auto dir : config_directories) {
        if (dir.directory_id.empty()) {
            // New directory - create on server
            auto status = CreateNewDirectory(dir);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
            
            // Save the new directory_id to config
            config_.UpdateDirectory(dir);
            
            config_dir_ids.insert(dir.directory_id);
            storage_.RegisterDirectory(dir.directory_id, dir.local_path);
            
            status = SubscribeToDirectory(dir.directory_id);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
            
            // Upload initial files for new directory
            status = UploadInitialFiles(dir);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
        } else {
            config_dir_ids.insert(dir.directory_id);
            
            if (storage_dir_ids.find(dir.directory_id) == storage_dir_ids.end()) {
                storage_.RegisterDirectory(dir.directory_id, dir.local_path);
            }
            
            auto status = SubscribeToDirectory(dir.directory_id);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
            
            status = SyncDirectory(dir.directory_id);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
        }
    }
    
    for (const auto& storage_dir_id : storage_dir_ids) {
        if (config_dir_ids.find(storage_dir_id) == config_dir_ids.end()) {
            storage_.UnregisterDirectory(storage_dir_id);
        }
    }

    return first_error;
}

absl::Status Synchronizer::CreateNewDirectory(DirectoryConfig& dir) {
    ClientMessage msg;
    msg.mutable_directory_create();

    auto response = grpc_client_.SendMessageWithResponse(msg);
    if (!response.ok()) {
        return response.status();
    }

    if (!response->has_ok_directory_created()) {
        return absl::InternalError("Unexpected response type");
    }

    dir.directory_id = response->ok_directory_created().directory_id();
    return absl::OkStatus();
}

absl::Status Synchronizer::UploadInitialFiles(const DirectoryConfig& dir) {
    namespace fs = std::filesystem;
    
    std::vector<FileChangeInfo> changes;
    auto now = std::chrono::system_clock::now();
    
    // Scan directory for all files
    try {
        for (const auto& entry : fs::recursive_directory_iterator(dir.local_path)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            
            // Get relative path
            fs::path relative = fs::relative(entry.path(), dir.local_path);
            
            FileChangeInfo info;
            info.file_id = std::nullopt;  // New file
            info.directory_id = dir.directory_id;
            info.current_path = relative;
            info.deleted = false;
            info.content_changed = true;
            info.first_try_time = now;
            
            changes.push_back(info);
        }
    } catch (const fs::filesystem_error& e) {
        return absl::InternalError("Failed to scan directory: " + std::string(e.what()));
    }
    
    if (changes.empty()) {
        return absl::OkStatus();  // No files to upload
    }
    
    return AskVersionIncrease(dir.directory_id, changes);
}

absl::Status Synchronizer::SubscribeToDirectory(const std::string& directory_id) {
    ClientMessage msg;
    auto* subscribe = msg.mutable_directory_subscribe();
    subscribe->set_directory_id(directory_id);

    auto response = grpc_client_.SendMessageWithResponse(msg);
    if (!response.ok()) {
        return response.status();
    }

    if (!response->has_ok_subscribed()) {
        return absl::InternalError("Unexpected response type");
    }

    std::lock_guard<std::mutex> lock(state_mutex_);
    directory_states_[directory_id].subscribed = true;

    return absl::OkStatus();
}

}  // namespace synxpo
