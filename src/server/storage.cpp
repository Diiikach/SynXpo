#include "synxpo/server/storage.h"
#include "synxpo/server/uuid.h"

#include <iostream>

namespace synxpo::server {

Storage::Storage() = default;
Storage::~Storage() = default;

std::string Storage::CreateDirectory() {
    std::unique_lock lock(mutex_);
    std::string dir_id = GenerateUuid();
    directories_[dir_id] = Directory{.id = dir_id};
    std::cout << "[Storage] Created directory: " << dir_id << std::endl;
    return dir_id;
}

bool Storage::DirectoryExists(const std::string& dir_id) const {
    std::shared_lock lock(mutex_);
    return directories_.count(dir_id) > 0;
}

std::vector<FileMetadata> Storage::GetDirectoryFiles(const std::string& dir_id) const {
    std::shared_lock lock(mutex_);
    std::vector<FileMetadata> result;
    
    auto it = directories_.find(dir_id);
    if (it == directories_.end()) {
        return result;
    }
    
    for (const auto& [file_id, file] : it->second.files) {
        if (file.deleted) continue;  // Don't return deleted files
        
        FileMetadata meta;
        meta.set_id(file.id);
        meta.set_directory_id(file.directory_id);
        meta.set_version(file.version);
        meta.set_content_changed_version(file.content_changed_version);
        meta.set_type(file.type);
        meta.set_current_path(file.current_path);
        meta.set_deleted(file.deleted);
        result.push_back(meta);
    }
    
    return result;
}

std::optional<StoredFile> Storage::GetFile(const std::string& dir_id,
                                            const std::string& file_id) const {
    std::shared_lock lock(mutex_);
    
    auto dir_it = directories_.find(dir_id);
    if (dir_it == directories_.end()) {
        return std::nullopt;
    }
    
    auto file_it = dir_it->second.files.find(file_id);
    if (file_it == dir_it->second.files.end()) {
        return std::nullopt;
    }
    
    return file_it->second;
}

std::optional<StoredFile> Storage::GetFileByPath(const std::string& dir_id,
                                                  const std::string& path) const {
    std::shared_lock lock(mutex_);
    
    auto dir_it = directories_.find(dir_id);
    if (dir_it == directories_.end()) {
        return std::nullopt;
    }
    
    auto path_it = dir_it->second.path_to_id.find(path);
    if (path_it == dir_it->second.path_to_id.end()) {
        return std::nullopt;
    }
    
    auto file_it = dir_it->second.files.find(path_it->second);
    if (file_it == dir_it->second.files.end()) {
        return std::nullopt;
    }
    
    return file_it->second;
}

std::vector<VersionCheckResult> Storage::CheckVersionIncrease(
    const std::string& client_id,
    const AskVersionIncrease& request) {
    
    std::unique_lock lock(mutex_);
    std::vector<VersionCheckResult> results;
    
    for (const auto& file_info : request.files()) {
        VersionCheckResult result;
        result.directory_id = file_info.directory_id();
        
        auto dir_it = directories_.find(file_info.directory_id());
        if (dir_it == directories_.end()) {
            // Directory doesn't exist
            result.status = DENIED;
            results.push_back(result);
            std::cout << "[Storage] CheckVersionIncrease: directory not found: " 
                      << file_info.directory_id() << std::endl;
            continue;
        }
        
        // Get FIRST_TRY_TIME from request
        uint64_t first_try_time = file_info.first_try_time().time();
        
        // Find existing file by ID or path
        StoredFile* existing_file = nullptr;
        std::string file_id;
        
        if (!file_info.id().empty()) {
            auto file_it = dir_it->second.files.find(file_info.id());
            if (file_it != dir_it->second.files.end()) {
                existing_file = &file_it->second;
                file_id = file_info.id();
            }
        }
        
        if (!existing_file && !file_info.current_path().empty()) {
            auto path_it = dir_it->second.path_to_id.find(file_info.current_path());
            if (path_it != dir_it->second.path_to_id.end()) {
                file_id = path_it->second;
                existing_file = &dir_it->second.files.at(file_id);
            }
        }
        
        result.file_id = file_id;
        
        if (existing_file) {
            // Implement LAST_TRY logic from spec:
            // 1. If LAST_TRY.time > FIRST_TRY_TIME -> DENIED
            // 2. If LAST_TRY.time < FIRST_TRY_TIME OR
            //    (LAST_TRY.time == FIRST_TRY_TIME AND same connection) -> check locks
            // 3. Otherwise -> DENIED
            
            const auto& last_try = existing_file->last_try;
            
            if (last_try.time > first_try_time) {
                // Another client started first
                result.status = DENIED;
                std::cout << "[Storage] CheckVersionIncrease: DENIED (last_try.time > first_try_time)" 
                          << std::endl;
            } else if (last_try.time < first_try_time ||
                       (last_try.time == first_try_time && last_try.connection_id == client_id)) {
                // We have priority or it's a retry
                
                // Check if file is locked
                if (existing_file->status == BLOCKED && 
                    existing_file->locked_by_client != client_id) {
                    result.status = BLOCKED;
                    std::cout << "[Storage] CheckVersionIncrease: BLOCKED by " 
                              << existing_file->locked_by_client << std::endl;
                } else if (existing_file->is_being_read) {
                    result.status = BLOCKED;
                    std::cout << "[Storage] CheckVersionIncrease: BLOCKED (being read)" << std::endl;
                } else {
                    result.status = FREE;
                    // Update LAST_TRY
                    existing_file->last_try.time = first_try_time;
                    existing_file->last_try.connection_id = client_id;
                }
            } else {
                // LAST_TRY.time == FIRST_TRY_TIME but different connection
                result.status = DENIED;
                std::cout << "[Storage] CheckVersionIncrease: DENIED (same time, different client)"
                          << std::endl;
            }
        } else {
            // New file - always allowed
            result.status = FREE;
            result.file_id = "";  // Will be assigned when created
            std::cout << "[Storage] CheckVersionIncrease: new file, FREE" << std::endl;
        }
        
        results.push_back(result);
    }
    
    return results;
}

void Storage::LockFilesForWrite(const std::string& client_id,
                                 const AskVersionIncrease& request) {
    std::unique_lock lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    
    for (const auto& file_info : request.files()) {
        auto dir_it = directories_.find(file_info.directory_id());
        if (dir_it == directories_.end()) continue;
        
        StoredFile* file = nullptr;
        
        if (!file_info.id().empty()) {
            auto file_it = dir_it->second.files.find(file_info.id());
            if (file_it != dir_it->second.files.end()) {
                file = &file_it->second;
            }
        } else if (!file_info.current_path().empty()) {
            auto path_it = dir_it->second.path_to_id.find(file_info.current_path());
            if (path_it != dir_it->second.path_to_id.end()) {
                file = &dir_it->second.files.at(path_it->second);
            }
        }
        
        if (file) {
            // Backup file before locking (for potential rollback)
            backups_[client_id][file->id] = *file;
            
            file->status = BLOCKED;
            file->locked_by_client = client_id;
            file->lock_time = now;
            std::cout << "[Storage] Locked file for write: " << file->id << std::endl;
        }
    }
}

std::vector<FileMetadata> Storage::ApplyVersionIncrease(
    const std::string& client_id,
    const AskVersionIncrease& request,
    const std::map<std::string, std::vector<uint8_t>>& file_contents) {
    
    std::unique_lock lock(mutex_);
    std::vector<FileMetadata> updated_files;
    
    for (const auto& file_info : request.files()) {
        auto dir_it = directories_.find(file_info.directory_id());
        if (dir_it == directories_.end()) {
            std::cerr << "[Storage] ApplyVersionIncrease: directory not found" << std::endl;
            continue;
        }
        
        std::string file_id = file_info.id();
        StoredFile* existing_file = nullptr;
        
        // Find existing file
        if (!file_id.empty()) {
            auto file_it = dir_it->second.files.find(file_id);
            if (file_it != dir_it->second.files.end()) {
                existing_file = &file_it->second;
            }
        } else if (!file_info.current_path().empty()) {
            auto path_it = dir_it->second.path_to_id.find(file_info.current_path());
            if (path_it != dir_it->second.path_to_id.end()) {
                file_id = path_it->second;
                existing_file = &dir_it->second.files.at(file_id);
            }
        }
        
        if (existing_file) {
            // Update existing file
            std::string old_path = existing_file->current_path;
            
            // Increment version (spec: versions start from 1)
            existing_file->version++;
            
            if (file_info.content_changed()) {
                existing_file->content_changed_version = existing_file->version;
                
                // Try to find content by file ID first, then by path
                auto content_it = file_contents.find(existing_file->id);
                if (content_it == file_contents.end()) {
                    content_it = file_contents.find(file_info.current_path());
                }
                if (content_it != file_contents.end()) {
                    existing_file->content = content_it->second;
                    std::cout << "[Storage] Updated file content: " << existing_file->id
                              << " size=" << existing_file->content.size() << std::endl;
                }
            }
            
            existing_file->current_path = file_info.current_path();
            existing_file->deleted = file_info.deleted();
            existing_file->type = file_info.type();
            existing_file->status = FREE;
            existing_file->locked_by_client.clear();
            
            // Update path index if path changed
            if (old_path != file_info.current_path()) {
                dir_it->second.path_to_id.erase(old_path);
            }
            if (!file_info.deleted()) {
                dir_it->second.path_to_id[file_info.current_path()] = file_id;
            } else {
                dir_it->second.path_to_id.erase(file_info.current_path());
            }
            
            // Build metadata response
            FileMetadata meta;
            meta.set_id(existing_file->id);
            meta.set_directory_id(existing_file->directory_id);
            meta.set_version(existing_file->version);
            meta.set_content_changed_version(existing_file->content_changed_version);
            meta.set_type(existing_file->type);
            meta.set_current_path(existing_file->current_path);
            meta.set_deleted(existing_file->deleted);
            updated_files.push_back(meta);
            
            std::cout << "[Storage] Updated file: " << existing_file->id 
                      << " path=" << existing_file->current_path 
                      << " version=" << existing_file->version 
                      << " content_changed_version=" << existing_file->content_changed_version
                      << std::endl;
        } else {
            // Create new file
            StoredFile new_file;
            new_file.id = GenerateUuid();
            new_file.directory_id = file_info.directory_id();
            new_file.version = 1;  // Versions start from 1 per spec
            new_file.content_changed_version = file_info.content_changed() ? 1 : 0;
            new_file.type = file_info.type();
            new_file.current_path = file_info.current_path();
            new_file.deleted = file_info.deleted();
            new_file.status = FREE;
            
            // Initialize LAST_TRY
            new_file.last_try.time = file_info.first_try_time().time();
            new_file.last_try.connection_id = client_id;
            
            if (file_info.content_changed()) {
                auto content_it = file_contents.find(file_info.current_path());
                if (content_it != file_contents.end()) {
                    new_file.content = content_it->second;
                    std::cout << "[Storage] New file content size=" << new_file.content.size() 
                              << std::endl;
                }
            }
            
            dir_it->second.files[new_file.id] = new_file;
            if (!new_file.deleted) {
                dir_it->second.path_to_id[new_file.current_path] = new_file.id;
            }
            
            FileMetadata meta;
            meta.set_id(new_file.id);
            meta.set_directory_id(new_file.directory_id);
            meta.set_version(new_file.version);
            meta.set_content_changed_version(new_file.content_changed_version);
            meta.set_type(new_file.type);
            meta.set_current_path(new_file.current_path);
            meta.set_deleted(new_file.deleted);
            updated_files.push_back(meta);
            
            std::cout << "[Storage] Created file: " << new_file.id 
                      << " path=" << new_file.current_path 
                      << " version=" << new_file.version << std::endl;
        }
    }
    
    // Clear backups for this client (successful commit)
    backups_.erase(client_id);
    
    return updated_files;
}

void Storage::RollbackUpload(const std::string& client_id,
                              const AskVersionIncrease& request) {
    std::unique_lock lock(mutex_);
    
    // Restore files from backup
    auto backup_it = backups_.find(client_id);
    if (backup_it != backups_.end()) {
        for (const auto& [file_id, backup] : backup_it->second) {
            auto dir_it = directories_.find(backup.directory_id);
            if (dir_it != directories_.end()) {
                auto file_it = dir_it->second.files.find(file_id);
                if (file_it != dir_it->second.files.end()) {
                    file_it->second = backup;
                    std::cout << "[Storage] Rolled back file: " << file_id << std::endl;
                }
            }
        }
        backups_.erase(backup_it);
    }
    
    // Also unlock any files from the request
    for (const auto& file_info : request.files()) {
        auto dir_it = directories_.find(file_info.directory_id());
        if (dir_it == directories_.end()) continue;
        
        StoredFile* file = nullptr;
        if (!file_info.id().empty()) {
            auto file_it = dir_it->second.files.find(file_info.id());
            if (file_it != dir_it->second.files.end()) {
                file = &file_it->second;
            }
        }
        
        if (file && file->locked_by_client == client_id) {
            file->status = FREE;
            file->locked_by_client.clear();
            std::cout << "[Storage] Unlocked file after rollback: " << file->id << std::endl;
        }
    }
}

std::vector<VersionCheckResult> Storage::CheckFilesForRead(
    const std::string& client_id,
    const RequestFileContent& request) {
    
    std::shared_lock lock(mutex_);
    std::vector<VersionCheckResult> results;
    
    for (const auto& file_id : request.files()) {
        VersionCheckResult result;
        result.file_id = file_id.id();
        result.directory_id = file_id.directory_id();
        
        auto dir_it = directories_.find(file_id.directory_id());
        if (dir_it == directories_.end()) {
            result.status = DENIED;
            results.push_back(result);
            continue;
        }
        
        auto file_it = dir_it->second.files.find(file_id.id());
        if (file_it == dir_it->second.files.end()) {
            result.status = DENIED;
            results.push_back(result);
            continue;
        }
        
        // Check if file is being written to
        if (file_it->second.status == BLOCKED) {
            result.status = BLOCKED;
        } else {
            result.status = FREE;
        }
        
        results.push_back(result);
    }
    
    return results;
}

void Storage::LockFilesForRead(const std::string& client_id,
                                const RequestFileContent& request) {
    std::unique_lock lock(mutex_);
    
    for (const auto& file_id : request.files()) {
        auto dir_it = directories_.find(file_id.directory_id());
        if (dir_it == directories_.end()) continue;
        
        auto file_it = dir_it->second.files.find(file_id.id());
        if (file_it != dir_it->second.files.end()) {
            file_it->second.is_being_read = true;
        }
    }
}

void Storage::UnlockFilesAfterRead(const std::string& client_id,
                                    const RequestFileContent& request) {
    std::unique_lock lock(mutex_);
    
    for (const auto& file_id : request.files()) {
        auto dir_it = directories_.find(file_id.directory_id());
        if (dir_it == directories_.end()) continue;
        
        auto file_it = dir_it->second.files.find(file_id.id());
        if (file_it != dir_it->second.files.end()) {
            file_it->second.is_being_read = false;
        }
    }
}

void Storage::ReleaseLocks(const std::string& client_id) {
    std::unique_lock lock(mutex_);
    
    for (auto& [dir_id, dir] : directories_) {
        for (auto& [file_id, file] : dir.files) {
            if (file.locked_by_client == client_id) {
                file.status = FREE;
                file.locked_by_client.clear();
                std::cout << "[Storage] Released lock on file: " << file_id << std::endl;
            }
        }
    }
    
    // Also clear any backups
    backups_.erase(client_id);
}

void Storage::CheckStaleLocks(std::chrono::seconds write_timeout) {
    std::unique_lock lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    
    for (auto& [dir_id, dir] : directories_) {
        for (auto& [file_id, file] : dir.files) {
            if (file.status == BLOCKED) {
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    now - file.lock_time);
                
                if (elapsed > write_timeout) {
                    std::cout << "[Storage] Releasing stale lock on file: " << file_id 
                              << " (held by " << file.locked_by_client << ")" << std::endl;
                    file.status = FREE;
                    file.locked_by_client.clear();
                }
            }
        }
    }
}

}  // namespace synxpo::server
