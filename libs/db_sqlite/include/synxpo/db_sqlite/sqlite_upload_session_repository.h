#pragma once

#include <filesystem>

#include "synxpo/db/upload_session_repository.h"

struct sqlite3;

namespace synxpo::db_sqlite {

class SqliteUploadSessionRepository final : public db::UploadSessionRepository {
public:
    static std::filesystem::path DefaultMigrationsDirectory();

    SqliteUploadSessionRepository(
        std::filesystem::path database_path,
        std::filesystem::path migrations_directory = DefaultMigrationsDirectory());
    ~SqliteUploadSessionRepository() override;

    SqliteUploadSessionRepository(const SqliteUploadSessionRepository&) = delete;
    SqliteUploadSessionRepository& operator=(const SqliteUploadSessionRepository&) = delete;

    db::RepositoryStatus Initialize();

    db::RepositoryStatus CreateDirectory(const domain::Directory& directory) override;
    std::optional<domain::Directory> GetDirectory(const std::string& directory_id) const override;
    db::RepositoryStatus CreateSession(
        const domain::UploadSession& session,
        const std::vector<domain::UploadSessionFile>& files) override;
    std::optional<domain::UploadSession> GetSession(const std::string& session_id) const override;
    std::vector<domain::UploadSessionFile> ListSessionFiles(const std::string& session_id) const override;
    db::RepositoryStatus TransitionSession(
        const std::string& session_id,
        domain::UploadSessionState expected_state,
        domain::UploadSessionState next_state,
        std::int64_t updated_at_ms) override;
    std::uint64_t ExpireSessions(std::int64_t now_ms) override;

private:
    db::RepositoryStatus Open();
    db::RepositoryStatus ApplyMigrations();
    db::RepositoryStatus Execute(const std::string& sql) const;

    std::filesystem::path database_path_;
    std::filesystem::path migrations_directory_;
    sqlite3* db_ = nullptr;
};

}  // namespace synxpo::db_sqlite
