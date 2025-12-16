#include "synxpo/client/synchronizer.h"
#include "synxpo/client/logger.h"

namespace synxpo {

absl::Status Synchronizer::InitializeDirectories() {
    LOG_INFO("InitializeDirectories: starting");
    auto config_directories = config_.GetDirectories();
    auto storage_directories = storage_.ListDirectories();

    std::set<std::string> storage_dir_ids(storage_directories.begin(), 
                                          storage_directories.end());
    
    std::set<std::string> config_dir_ids;
    absl::Status first_error = absl::OkStatus();
    
    for (auto& dir : config_directories) {
        if (dir.directory_id.empty()) {
            // New directory - create on server
            LOG_INFO("Creating new directory: " + dir.local_path.string());
            auto status = CreateNewDirectory(dir);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
            
            // Update config with new directory_id
            config_.UpdateDirectory(dir);
            LOG_INFO("Directory created with ID: " + dir.directory_id);
            
            config_dir_ids.insert(dir.directory_id);
            storage_.RegisterDirectory(dir.directory_id, dir.local_path);
            
            status = SubscribeToDirectory(dir.directory_id);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
            
            // Upload all local files for new directory
            status = UploadLocalFiles(dir.directory_id);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
            
            status = SyncDirectory(dir.directory_id);
            if (!status.ok()) {
                first_error.Update(status);
                continue;
            }
        } else {
            config_dir_ids.insert(dir.directory_id);
            LOG_INFO("Using existing directory: " + dir.local_path.string() + " (" + dir.directory_id + ")");
            
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
    
    // Save config to persist any new directory_ids
    if (!config_path_.empty()) {
        auto save_status = config_.Save(config_path_);
        if (!save_status.ok()) {
            LOG_WARNING("Failed to save config after initialization: " + std::string(save_status.message()));
        } else {
            LOG_INFO("Configuration saved successfully");
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

absl::Status Synchronizer::UploadLocalFiles(const std::string& directory_id) {
    LOG_INFO("UploadLocalFiles: directory_id=" + directory_id);
    
    auto dir_path = GetDirectoryPath(directory_id);
    if (!dir_path.has_value()) {
        return absl::NotFoundError("Directory not found in config");
    }
    
    if (!std::filesystem::exists(*dir_path)) {
        LOG_WARNING("Directory path does not exist: " + dir_path->string());
        return absl::OkStatus();  // Nothing to upload
    }
    
    std::vector<FileChangeInfo> changes;
    
    try {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(*dir_path)) {
            if (entry.is_regular_file()) {
                FileChangeInfo info;
                info.directory_id = directory_id;
                // Store relative path from directory root
                info.current_path = std::filesystem::relative(entry.path(), *dir_path);
                info.deleted = false;
                info.content_changed = true;
                info.first_try_time = std::chrono::system_clock::now();
                
                changes.push_back(info);
                LOG_DEBUG("Found local file: " + entry.path().string() + " (relative: " + info.current_path.string() + ")");
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return absl::InternalError("Failed to scan directory: " + std::string(e.what()));
    }
    
    if (changes.empty()) {
        LOG_INFO("No local files to upload");
        return absl::OkStatus();
    }
    
    LOG_INFO("Uploading " + std::to_string(changes.size()) + " local files");
    
    // Send ASK_VERSION_INCREASE for all files
    auto status = AskVersionIncrease(directory_id, changes);
    if (!status.ok()) {
        return status;
    }
    
    // Upload will happen when we receive VERSION_INCREASE_ALLOW response
    return absl::OkStatus();
}

}  // namespace synxpo
