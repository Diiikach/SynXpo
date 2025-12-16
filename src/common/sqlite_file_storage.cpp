#include "synxpo/common/sqlite_file_storage.h"

#include <system_error>

#include <absl/status/status.h>
#include <sqlite3.h>

namespace synxpo {
namespace {

absl::Status ToStatus(int rc, sqlite3* db, const std::string& context) {
    if (rc == SQLITE_OK || rc == SQLITE_DONE || rc == SQLITE_ROW) {
        return absl::OkStatus();
    }

    const char* msg = db ? sqlite3_errmsg(db) : "unknown sqlite error";
    return absl::InternalError(context + ": " + msg);
}

absl::StatusOr<std::filesystem::path> AbsPath(const std::filesystem::path& p) {
    std::error_code ec;
    auto abs = std::filesystem::absolute(p, ec);
    if (ec) {
        return absl::InvalidArgumentError("Failed to compute absolute path: " + p.string());
    }
    return abs;
}

absl::StatusOr<FileMetadata> ParseMetadata(const void* data, int size) {
    FileMetadata meta;
    if (data == nullptr || size <= 0) {
        return absl::InternalError("Empty metadata blob");
    }
    if (!meta.ParseFromArray(data, size)) {
        return absl::InternalError("Failed to parse FileMetadata blob");
    }
    return meta;
}

}  // namespace

SqliteFileMetadataStorage::SqliteFileMetadataStorage(const std::filesystem::path& db_path) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto abs_db_path = AbsPath(db_path);
    if (!abs_db_path.ok()) {
        return;
    }

    std::error_code ec;
    std::filesystem::create_directories(abs_db_path->parent_path(), ec);

    const int rc = sqlite3_open(abs_db_path->string().c_str(), &db_);
    if (rc != SQLITE_OK) {
        sqlite3_close(db_);
        db_ = nullptr;
        return;
    }

    (void)ExecLocked("PRAGMA foreign_keys = ON;");
    (void)ExecLocked("PRAGMA journal_mode = WAL;");

    (void)InitSchemaLocked();
    (void)LoadDirectoriesLocked();
}

SqliteFileMetadataStorage::~SqliteFileMetadataStorage() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

absl::Status SqliteFileMetadataStorage::ExecLocked(const char* sql) const {
    char* err = nullptr;
    const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        std::string msg = err ? err : "unknown sqlite exec error";
        sqlite3_free(err);
        return absl::InternalError(msg);
    }
    return absl::OkStatus();
}

absl::Status SqliteFileMetadataStorage::InitSchemaLocked() {
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    const char* schema_sql =
        "BEGIN;"
        "CREATE TABLE IF NOT EXISTS directories ("
        "  directory_id TEXT PRIMARY KEY,"
        "  directory_path TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS files ("
        "  directory_id TEXT NOT NULL,"
        "  file_id TEXT NOT NULL,"
        "  current_path TEXT NOT NULL,"
        "  metadata BLOB NOT NULL,"
        "  PRIMARY KEY(directory_id, file_id),"
        "  FOREIGN KEY(directory_id) REFERENCES directories(directory_id) ON DELETE CASCADE"
        ");"
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_files_dir_path ON files(directory_id, current_path);"
        "COMMIT;";

    return ExecLocked(schema_sql);
}

absl::Status SqliteFileMetadataStorage::LoadDirectoriesLocked() {
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    directories_.clear();

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT directory_id, directory_path FROM directories";
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ToStatus(rc, db_, "Prepare LoadDirectories");
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* id = sqlite3_column_text(stmt, 0);
        const unsigned char* path = sqlite3_column_text(stmt, 1);
        if (id && path) {
            directories_[reinterpret_cast<const char*>(id)] =
                std::filesystem::path(reinterpret_cast<const char*>(path));
        }
    }

    sqlite3_finalize(stmt);
    return ToStatus(rc, db_, "Step LoadDirectories");
}

void SqliteFileMetadataStorage::RegisterDirectory(
    const std::string& directory_id,
    const std::filesystem::path& directory_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return;
    }

    auto abs_path = AbsPath(directory_path);
    if (!abs_path.ok()) {
        return;
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "INSERT INTO directories(directory_id, directory_path) VALUES(?, ?) "
        "ON CONFLICT(directory_id) DO UPDATE SET directory_path=excluded.directory_path";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return;
    }

    sqlite3_bind_text(stmt, 1, directory_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, abs_path->string().c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return;
    }

    directories_[directory_id] = *abs_path;
}

void SqliteFileMetadataStorage::UnregisterDirectory(const std::string& directory_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return;
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "DELETE FROM directories WHERE directory_id = ?";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return;
    }

    sqlite3_bind_text(stmt, 1, directory_id.c_str(), -1, SQLITE_TRANSIENT);
    (void)sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    directories_.erase(directory_id);
}

std::vector<std::string> SqliteFileMetadataStorage::ListDirectories() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> result;
    result.reserve(directories_.size());
    for (const auto& [dir_id, _] : directories_) {
        result.push_back(dir_id);
    }
    return result;
}

absl::StatusOr<std::vector<FileMetadata>> SqliteFileMetadataStorage::ListDirectoryFiles(
    const std::string& directory_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    if (directories_.find(directory_id) == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT metadata FROM files WHERE directory_id = ?";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ToStatus(rc, db_, "Prepare ListDirectoryFiles");
    }

    sqlite3_bind_text(stmt, 1, directory_id.c_str(), -1, SQLITE_TRANSIENT);

    std::vector<FileMetadata> result;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const void* blob = sqlite3_column_blob(stmt, 0);
        const int size = sqlite3_column_bytes(stmt, 0);

        auto meta_or = ParseMetadata(blob, size);
        if (!meta_or.ok()) {
            sqlite3_finalize(stmt);
            return meta_or.status();
        }
        result.push_back(*meta_or);
    }

    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return ToStatus(rc, db_, "Step ListDirectoryFiles");
    }

    return result;
}

std::optional<std::string> SqliteFileMetadataStorage::GetDirectoryIdByPath(
    const std::filesystem::path& file_path) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::error_code ec;
    const auto absolute_path = std::filesystem::absolute(file_path, ec);
    if (ec) {
        return std::nullopt;
    }

    for (const auto& [dir_id, dir_path] : directories_) {
        const auto rel_path = std::filesystem::relative(absolute_path, dir_path, ec);
        if (ec) {
            continue;
        }

        const auto rel_str = rel_path.string();
        if (rel_str.size() >= 2 && rel_str.substr(0, 2) == "..") {
            continue;
        }

        return dir_id;
    }

    return std::nullopt;
}

absl::StatusOr<FileMetadata> SqliteFileMetadataStorage::GetFileMetadata(
    const std::string& directory_id,
    const std::string& file_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    if (directories_.find(directory_id) == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "SELECT metadata FROM files WHERE directory_id = ? AND file_id = ?";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ToStatus(rc, db_, "Prepare GetFileMetadata(by id)");
    }

    sqlite3_bind_text(stmt, 1, directory_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, file_id.c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const void* blob = sqlite3_column_blob(stmt, 0);
        const int size = sqlite3_column_bytes(stmt, 0);
        auto meta_or = ParseMetadata(blob, size);
        sqlite3_finalize(stmt);
        return meta_or;
    }

    sqlite3_finalize(stmt);
    if (rc == SQLITE_DONE) {
        return absl::NotFoundError("File not found: " + file_id);
    }

    return ToStatus(rc, db_, "Step GetFileMetadata(by id)");
}

absl::StatusOr<FileMetadata> SqliteFileMetadataStorage::GetFileMetadata(
    const std::string& directory_id,
    const std::filesystem::path& path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    if (directories_.find(directory_id) == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "SELECT metadata FROM files WHERE directory_id = ? AND current_path = ?";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ToStatus(rc, db_, "Prepare GetFileMetadata(by path)");
    }

    sqlite3_bind_text(stmt, 1, directory_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, path.string().c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const void* blob = sqlite3_column_blob(stmt, 0);
        const int size = sqlite3_column_bytes(stmt, 0);
        auto meta_or = ParseMetadata(blob, size);
        sqlite3_finalize(stmt);
        return meta_or;
    }

    sqlite3_finalize(stmt);
    if (rc == SQLITE_DONE) {
        return absl::NotFoundError("File not found at path: " + path.string());
    }

    return ToStatus(rc, db_, "Step GetFileMetadata(by path)");
}

absl::Status SqliteFileMetadataStorage::UpsertFile(const FileMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    if (!metadata.has_id() || metadata.id().empty()) {
        return absl::InvalidArgumentError("File ID is required");
    }

    if (metadata.directory_id().empty()) {
        return absl::InvalidArgumentError("Directory ID is required");
    }

    if (directories_.find(metadata.directory_id()) == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + metadata.directory_id());
    }

    std::string blob;
    if (!metadata.SerializeToString(&blob)) {
        return absl::InternalError("Failed to serialize FileMetadata");
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "INSERT INTO files(directory_id, file_id, current_path, metadata) VALUES(?, ?, ?, ?) "
        "ON CONFLICT(directory_id, file_id) DO UPDATE SET "
        "  current_path=excluded.current_path,"
        "  metadata=excluded.metadata";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ToStatus(rc, db_, "Prepare UpsertFile");
    }

    sqlite3_bind_text(stmt, 1, metadata.directory_id().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, metadata.id().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, metadata.current_path().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt, 4, blob.data(), static_cast<int>(blob.size()), SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return ToStatus(rc, db_, "Step UpsertFile");
    }

    return absl::OkStatus();
}

absl::Status SqliteFileMetadataStorage::RemoveFile(
    const std::string& directory_id,
    const std::string& file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) {
        return absl::FailedPreconditionError("SQLite DB is not open");
    }

    if (directories_.find(directory_id) == directories_.end()) {
        return absl::NotFoundError("Directory not found: " + directory_id);
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "DELETE FROM files WHERE directory_id = ? AND file_id = ?";

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ToStatus(rc, db_, "Prepare RemoveFile");
    }

    sqlite3_bind_text(stmt, 1, directory_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, file_id.c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    const int changes = sqlite3_changes(db_);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return ToStatus(rc, db_, "Step RemoveFile");
    }

    if (changes == 0) {
        return absl::NotFoundError("File not found: " + file_id);
    }

    return absl::OkStatus();
}

}  // namespace synxpo
