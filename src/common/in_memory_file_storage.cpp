#include "synxpo/common/in_memory_file_storage.h"

#include <absl/status/status.h>

namespace synxpo {

void InMemoryFileMetadataStorage::RegisterDirectory(
    const std::string& directory_id,
    const std::filesystem::path& directory_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    DirectoryInfo info;
    info.path = directory_path;
    directories_[directory_id] = std::move(info);
}

void InMemoryFileMetadataStorage::UnregisterDirectory(const std::string& directory_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    directories_.erase(directory_id);
}

std::vector<std::string> InMemoryFileMetadataStorage::ListDirectories() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<std::string> result;
    result.reserve(directories_.size());
    
    for (const auto& [dir_id, _] : directories_) {
        result.push_back(dir_id);
    }
    
    return result;
}

absl::StatusOr<std::vector<FileMetadata>> InMemoryFileMetadataStorage::ListDirectoryFiles(
    const std::string& directory_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = directories_.find(directory_id);
    if (it == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }
    
    std::vector<FileMetadata> result;
    result.reserve(it->second.files.size());
    
    for (const auto& [file_id, metadata] : it->second.files) {
        result.push_back(metadata);
    }
    
    return result;
}

std::optional<std::string> InMemoryFileMetadataStorage::GetDirectoryIdByPath(
    const std::filesystem::path& file_path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::error_code ec;
    const auto absolute_path = std::filesystem::absolute(file_path, ec);
    if (ec) {
        return std::nullopt;
    }
    
    for (const auto& [dir_id, dir_info] : directories_) {
        const auto rel_path = std::filesystem::relative(absolute_path, dir_info.path, ec);
        if (ec) {
            continue;
        }
        
        // Check that the path doesn't escape the directory (no ".." prefix)
        const auto rel_str = rel_path.string();
        if (rel_str.size() >= 2 && rel_str.substr(0, 2) == "..") {
            continue;
        }
        
        return dir_id;
    }
    
    return std::nullopt;
}

absl::StatusOr<FileMetadata> InMemoryFileMetadataStorage::GetFileMetadata(
    const std::string& directory_id,
    const std::string& file_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto dir_it = directories_.find(directory_id);
    if (dir_it == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }
    
    auto file_it = dir_it->second.files.find(file_id);
    if (file_it == dir_it->second.files.end()) {
        return absl::NotFoundError("File not found: " + file_id);
    }
    
    return file_it->second;
}

absl::StatusOr<FileMetadata> InMemoryFileMetadataStorage::GetFileMetadata(
    const std::string& directory_id,
    const std::filesystem::path& path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto dir_it = directories_.find(directory_id);
    if (dir_it == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }
    
    auto path_it = dir_it->second.path_to_id.find(path);
    if (path_it == dir_it->second.path_to_id.end()) {
        return absl::NotFoundError("File not found at path: " + path.string());
    }
    
    const std::string& file_id = path_it->second;
    auto file_it = dir_it->second.files.find(file_id);
    if (file_it == dir_it->second.files.end()) {
        return absl::InternalError("Inconsistent state: path mapping exists but file not found");
    }
    
    return file_it->second;
}

absl::Status InMemoryFileMetadataStorage::UpsertFile(const FileMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!metadata.has_id() || metadata.id().empty()) {
        return absl::InvalidArgumentError("File ID is required");
    }
    
    if (metadata.directory_id().empty()) {
        return absl::InvalidArgumentError("Directory ID is required");
    }
    
    auto dir_it = directories_.find(metadata.directory_id());
    if (dir_it == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + metadata.directory_id());
    }
    
    DirectoryInfo& dir_info = dir_it->second;
    
    auto file_it = dir_info.files.find(metadata.id());
    if (file_it != dir_info.files.end()) {
        const FileMetadata& old_metadata = file_it->second;
        if (old_metadata.current_path() != metadata.current_path()) {
            dir_info.path_to_id.erase(old_metadata.current_path());
        }
    }
    
    dir_info.files[metadata.id()] = metadata;
    dir_info.path_to_id[metadata.current_path()] = metadata.id();
    
    return absl::OkStatus();
}

absl::Status InMemoryFileMetadataStorage::RemoveFile(
    const std::string& directory_id,
    const std::string& file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto dir_it = directories_.find(directory_id);
    if (dir_it == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }
    
    DirectoryInfo& dir_info = dir_it->second;
    
    auto file_it = dir_info.files.find(file_id);
    if (file_it == dir_info.files.end()) {
        return absl::NotFoundError("File not found: " + file_id);
    }
    
    const FileMetadata& metadata = file_it->second;
    dir_info.path_to_id.erase(metadata.current_path());
    dir_info.files.erase(file_it);
    
    return absl::OkStatus();
}

}  // namespace synxpo
