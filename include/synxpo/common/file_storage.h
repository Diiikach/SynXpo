#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "synxpo.pb.h"

namespace synxpo {

/* This interface represents the storage of file metadata. */
/* It does not keep track of the filesystem, that's up to the user.  */
class IFileMetadataStorage {
public:
    virtual ~IFileMetadataStorage() = default;
    
    // Directory management
    virtual void RegisterDirectory(const std::string& directory_id, 
                                   const std::filesystem::path& directory_path) = 0;
    virtual void UnregisterDirectory(const std::string& directory_id) = 0;
    virtual std::vector<std::string> ListDirectories() const = 0;
    virtual absl::StatusOr<std::vector<FileMetadata>> ListDirectoryFiles(
        const std::string& directory_id) const = 0;
    
    // Get directory ID by file path
    virtual std::optional<std::string> GetDirectoryIdByPath(
        const std::filesystem::path& file_path) const = 0;
    
    // Get file metadata by id
    virtual absl::StatusOr<FileMetadata> GetFileMetadata(
        const std::string& file_id,
        const std::string& directory_id) const = 0;

    // Get file metadata by path
    virtual absl::StatusOr<FileMetadata> GetFileMetadata(
        const std::filesystem::path& path,
        const std::string& directory_id) const = 0;

    // Update file metadata
    virtual absl::Status UpsertFile(const FileMetadata& metadata) = 0;

    // Remove file
    virtual absl::Status RemoveFile(
        const std::string& file_id, 
        const std::string& directory_id) = 0;
};

}  // namespace synxpo
