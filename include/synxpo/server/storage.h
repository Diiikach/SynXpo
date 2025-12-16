#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "synxpo/common/file_storage.h"
#include "synxpo.pb.h"

namespace synxpo::server {

/// Information about the last attempt to modify a file
struct LastTry {
    uint64_t time = 0;             // FIRST_TRY_TIME from the request
    std::string connection_id;      // Client who made the request
};

/// A file stored on the server
struct StoredFile {
    std::string id;
    std::string directory_id;
    uint64_t version = 0;                         // Versions start from 1
    uint64_t content_changed_version = 0;
    FileType type = FILE;
    std::string current_path;
    bool deleted = false;
    std::vector<uint8_t> content;
    
    // Locking state
    FileStatus status = FREE;
    std::string locked_by_client;
    std::chrono::steady_clock::time_point lock_time;
    bool is_being_read = false;
    
    // For conflict resolution per spec
    LastTry last_try;
};

/// A synchronized directory
struct Directory {
    std::string id;
    std::map<std::string, StoredFile> files;       // file_id -> file
    std::map<std::string, std::string> path_to_id; // path -> file_id
};

/// Result of checking if a version increase is allowed
struct VersionCheckResult {
    std::string file_id;
    FileStatus status = FREE;
    std::string directory_id;
};

/// Thread-safe storage for server data
/// Stores file content on disk, metadata via IFileMetadataStorage
class Storage {
public:
    /// Create storage with given base directory for files and metadata storage
    Storage(const std::filesystem::path& storage_root,
            std::shared_ptr<IFileMetadataStorage> metadata_storage);
    ~Storage();
    
    /// Create a new directory, returns its ID
    std::string CreateDirectory();
    
    /// Register existing directory by path, returns its ID
    std::string RegisterDirectory(const std::filesystem::path& dir_path);
    
    /// Check if a directory exists
    bool DirectoryExists(const std::string& dir_id) const;
    
    /// Get metadata for all files in a directory
    std::vector<FileMetadata> GetDirectoryFiles(const std::string& dir_id) const;
    
    /// Get a specific file by ID
    std::optional<StoredFile> GetFile(const std::string& dir_id, 
                                       const std::string& file_id) const;
    
    /// Get a file by its path within a directory
    std::optional<StoredFile> GetFileByPath(const std::string& dir_id,
                                             const std::string& path) const;
    
    /// Check if version increase is allowed for the given files
    /// Implements LAST_TRY logic from the specification
    std::vector<VersionCheckResult> CheckVersionIncrease(
        const std::string& client_id,
        const AskVersionIncrease& request);
    
    /// Lock files for writing (after CheckVersionIncrease returned all FREE)
    void LockFilesForWrite(const std::string& client_id,
                           const AskVersionIncrease& request);
    
    /// Apply version increase after content has been received
    /// Returns metadata of updated files
    std::vector<FileMetadata> ApplyVersionIncrease(
        const std::string& client_id,
        const AskVersionIncrease& request,
        const std::map<std::string, std::vector<uint8_t>>& file_contents);
    
    /// Rollback a pending upload (on timeout or error)
    void RollbackUpload(const std::string& client_id,
                        const AskVersionIncrease& request);
    
    /// Check if files can be read (not being written to)
    std::vector<VersionCheckResult> CheckFilesForRead(
        const std::string& client_id,
        const RequestFileContent& request);
    
    /// Lock files for reading
    void LockFilesForRead(const std::string& client_id,
                          const RequestFileContent& request);
    
    /// Unlock files after reading
    void UnlockFilesAfterRead(const std::string& client_id,
                              const RequestFileContent& request);
    
    /// Release all locks held by a client (on disconnect)
    void ReleaseLocks(const std::string& client_id);
    
    /// Check and release stale locks (for timeout handling)
    void CheckStaleLocks(std::chrono::seconds write_timeout);

private:
    /// Get the disk path for a file
    std::filesystem::path GetFileDiskPath(const std::string& dir_id, 
                                           const std::string& file_id) const;
    
    /// Read file content from disk
    std::vector<uint8_t> ReadFileContent(const std::string& dir_id,
                                          const std::string& file_id) const;
    
    /// Write file content to disk
    void WriteFileContent(const std::string& dir_id,
                          const std::string& file_id,
                          const std::vector<uint8_t>& content);
    
    /// Delete file from disk
    void DeleteFileFromDisk(const std::string& dir_id, const std::string& file_id);

    std::filesystem::path storage_root_;
    std::shared_ptr<IFileMetadataStorage> metadata_storage_;
    
    mutable std::shared_mutex mutex_;
    std::map<std::string, Directory> directories_;
    
    // Backup storage for rollback
    std::map<std::string, std::map<std::string, StoredFile>> backups_; // client_id -> file_id -> backup
};

}  // namespace synxpo::server
