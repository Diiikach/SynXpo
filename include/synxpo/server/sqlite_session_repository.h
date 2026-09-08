#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "synxpo/server/upload_session.h"

struct sqlite3;

namespace synxpo::server {

enum class RepositoryError {
    kOk,
    kInvalidArgument,
    kNotFound,
    kConflict,
    kStorage,
};

struct RepositoryStatus {
    RepositoryError code = RepositoryError::kOk;
    std::string message;

    [[nodiscard]] bool ok() const { return code == RepositoryError::kOk; }
    static RepositoryStatus Ok() { return {}; }
};

class SqliteSessionRepository {
public:
    explicit SqliteSessionRepository(const std::filesystem::path& database_path);
    ~SqliteSessionRepository();

    SqliteSessionRepository(const SqliteSessionRepository&) = delete;
    SqliteSessionRepository& operator=(const SqliteSessionRepository&) = delete;

    RepositoryStatus Initialize();

    RepositoryStatus CreateDirectory(const Directory& directory);
    std::optional<Directory> GetDirectory(const std::string& directory_id) const;

    // Creates the session and immutable manifest in one SQLite transaction.
    RepositoryStatus CreateSession(
        const UploadSession& session,
        const std::vector<UploadSessionFile>& files);
    std::optional<UploadSession> GetSession(const std::string& session_id) const;
    std::vector<UploadSessionFile> ListSessionFiles(const std::string& session_id) const;

    // Performs a compare-and-set state transition. A stale state is a conflict.
    RepositoryStatus TransitionSession(
        const std::string& session_id,
        UploadSessionState expected_state,
        UploadSessionState next_state,
        std::int64_t updated_at_ms);

    // Expires non-terminal sessions whose lease has elapsed. Returns their count.
    std::uint64_t ExpireSessions(std::int64_t now_ms);

private:
    RepositoryStatus Open();
    RepositoryStatus ApplyMigrations();
    RepositoryStatus Execute(const char* sql) const;

    std::filesystem::path database_path_;
    sqlite3* db_ = nullptr;
};

}  // namespace synxpo::server
