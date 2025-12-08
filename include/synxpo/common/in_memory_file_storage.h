#pragma once

#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "synxpo/common/file_storage.h"

namespace synxpo {

class InMemoryFileMetadataStorage : public IFileMetadataStorage {
public:
    InMemoryFileMetadataStorage() = default;
    ~InMemoryFileMetadataStorage() override = default;

    InMemoryFileMetadataStorage(const InMemoryFileMetadataStorage&) = delete;
    InMemoryFileMetadataStorage& operator=(const InMemoryFileMetadataStorage&) = delete;
    InMemoryFileMetadataStorage(InMemoryFileMetadataStorage&&) = delete;
    InMemoryFileMetadataStorage& operator=(InMemoryFileMetadataStorage&&) = delete;
    
    // Directory management
    void RegisterDirectory(const std::string& directory_id, 
                          const std::filesystem::path& directory_path) override;
    void UnregisterDirectory(const std::string& directory_id) override;
    std::vector<std::string> ListDirectories() const override;
    absl::StatusOr<std::vector<FileMetadata>> ListDirectoryFiles(
        const std::string& directory_id) const override;
    
    // Get directory ID by file path
    std::optional<std::string> GetDirectoryIdByPath(
        const std::filesystem::path& file_path) const override;
    
    // Get file metadata by id
    absl::StatusOr<FileMetadata> GetFileMetadata(
        const std::string& file_id,
        const std::string& directory_id) const override;

    // Get file metadata by path
    absl::StatusOr<FileMetadata> GetFileMetadata(
        const std::filesystem::path& path,
        const std::string& directory_id) const override;

    // Update file metadata
    absl::Status UpsertFile(const FileMetadata& metadata) override;

    // Remove file
    absl::Status RemoveFile(
        const std::string& file_id, 
        const std::string& directory_id) override;

private:
    struct DirectoryInfo {
        std::filesystem::path path;
        std::unordered_map<std::string, FileMetadata> files;
        std::map<std::filesystem::path, std::string> path_to_id;
    };
    
    mutable std::mutex mutex_;
    std::unordered_map<std::string, DirectoryInfo> directories_;
};

}  // namespace synxpo
