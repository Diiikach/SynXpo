#pragma once

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "synxpo/common/file_storage.h"

struct sqlite3;

namespace synxpo {

class SqliteFileMetadataStorage : public IFileMetadataStorage {
public:
    explicit SqliteFileMetadataStorage(const std::filesystem::path& db_path);
    ~SqliteFileMetadataStorage() override;

    SqliteFileMetadataStorage(const SqliteFileMetadataStorage&) = delete;
    SqliteFileMetadataStorage& operator=(const SqliteFileMetadataStorage&) = delete;
    SqliteFileMetadataStorage(SqliteFileMetadataStorage&&) = delete;
    SqliteFileMetadataStorage& operator=(SqliteFileMetadataStorage&&) = delete;

    void RegisterDirectory(const std::string& directory_id,
                           const std::filesystem::path& directory_path) override;
    void UnregisterDirectory(const std::string& directory_id) override;
    std::vector<std::string> ListDirectories() const override;
    absl::StatusOr<std::vector<FileMetadata>> ListDirectoryFiles(
        const std::string& directory_id) const override;

    std::optional<std::string> GetDirectoryIdByPath(
        const std::filesystem::path& file_path) const override;

    absl::StatusOr<FileMetadata> GetFileMetadata(
        const std::string& directory_id,
        const std::string& file_id) const override;

    absl::StatusOr<FileMetadata> GetFileMetadata(
        const std::string& directory_id,
        const std::filesystem::path& path) const override;

    absl::Status UpsertFile(const FileMetadata& metadata) override;

    absl::Status RemoveFile(
        const std::string& directory_id,
        const std::string& file_id) override;

private:
    absl::Status InitSchemaLocked();
    absl::Status LoadDirectoriesLocked();

    absl::Status ExecLocked(const char* sql) const;

    mutable std::mutex mutex_;
    sqlite3* db_ = nullptr;

    std::unordered_map<std::string, std::filesystem::path> directories_;
};

}  // namespace synxpo
