#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "synxpo/db_sqlite/sqlite_upload_session_repository.h"

namespace {

using synxpo::db::RepositoryError;
using synxpo::db_sqlite::SqliteUploadSessionRepository;
using synxpo::domain::Directory;
using synxpo::domain::UploadFileOperation;
using synxpo::domain::UploadFileState;
using synxpo::domain::UploadSession;
using synxpo::domain::UploadSessionFile;
using synxpo::domain::UploadSessionState;

void Require(bool value, const std::string& message) {
    if (!value) throw std::runtime_error(message);
}

struct TemporaryDirectory {
    TemporaryDirectory() {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        path = std::filesystem::temp_directory_path() / ("synxpo-session-test-" + std::to_string(now));
        std::filesystem::create_directories(path);
    }
    ~TemporaryDirectory() { std::filesystem::remove_all(path); }
    std::filesystem::path path;
};

UploadSession NewSession(const std::string& id, std::int64_t now_ms = 1'000) {
    UploadSession session;
    session.id = id;
    session.directory_id = "directory-1";
    session.owner_id = "owner-1";
    session.base_revision = 3;
    session.manifest_hash = "manifest-sha256";
    session.idempotency_key = "request-" + id;
    session.created_at_ms = now_ms;
    session.updated_at_ms = now_ms;
    session.expires_at_ms = now_ms + 10'000;
    return session;
}

void AddDirectory(SqliteUploadSessionRepository& repository) {
    const auto status = repository.CreateDirectory(Directory{"directory-1", 3, 100});
    Require(status.ok(), "directory must be created: " + status.message);
}

int ReadAppliedMigrationVersion(const std::filesystem::path& database_path) {
    sqlite3* database = nullptr;
    Require(sqlite3_open_v2(database_path.string().c_str(), &database, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK,
            "database must be readable");
    sqlite3_stmt* statement = nullptr;
    Require(sqlite3_prepare_v2(database, "SELECT MAX(version) FROM schema_migrations;", -1, &statement, nullptr) == SQLITE_OK,
            "schema_migrations must exist");
    Require(sqlite3_step(statement) == SQLITE_ROW, "schema migration version must be readable");
    const int version = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    sqlite3_close(database);
    return version;
}

void TestSchemaAndPersistence() {
    TemporaryDirectory temporary_directory;
    const auto database_path = temporary_directory.path / "server.db";

    {
        SqliteUploadSessionRepository repository(database_path);
        Require(repository.Initialize().ok(), "migrations must succeed");
        Require(ReadAppliedMigrationVersion(database_path) == 1, "initial migration must be recorded");
        AddDirectory(repository);

        auto session = NewSession("session-1");
        UploadSessionFile create_file{
            .session_id = session.id,
            .path = "new.txt",
            .operation = UploadFileOperation::kCreate,
            .content_hash = "file-sha256",
            .size = 12,
            .staging_path = "staging/session-1/new.txt",
            .state = UploadFileState::kPending,
        };
        UploadSessionFile delete_file{
            .session_id = session.id,
            .file_id = "old-file",
            .path = "old.txt",
            .operation = UploadFileOperation::kDelete,
            .expected_version = 3,
        };
        const auto create_status = repository.CreateSession(session, {create_file, delete_file});
        Require(create_status.ok(), "session and manifest must be stored: " + create_status.message);

        const auto stored = repository.GetSession(session.id);
        Require(stored.has_value(), "stored session must be readable");
        Require(stored->state == UploadSessionState::kCreated, "new session state must be created");
        Require(stored->base_revision == 3, "base revision must persist");
        Require(stored->manifest_hash == session.manifest_hash, "manifest hash must persist");
        const auto files = repository.ListSessionFiles(session.id);
        Require(files.size() == 2, "manifest files must persist");
        Require(files[0].path == "new.txt" && files[1].path == "old.txt", "files must be sorted by path");
    }

    SqliteUploadSessionRepository reopened_repository(database_path);
    Require(reopened_repository.Initialize().ok(), "migrations must be idempotent");
    Require(reopened_repository.GetSession("session-1").has_value(), "session must survive a restart");
}

void TestAdditionalMigrationIsAppliedOnce() {
    TemporaryDirectory temporary_directory;
    const auto migrations_directory = temporary_directory.path / "migrations";
    std::filesystem::copy(
        SqliteUploadSessionRepository::DefaultMigrationsDirectory(), migrations_directory,
        std::filesystem::copy_options::recursive);
    {
        std::ofstream migration(migrations_directory / "0002_test_table.sql");
        migration << "CREATE TABLE migration_test_table (id INTEGER PRIMARY KEY);\n";
    }

    const auto database_path = temporary_directory.path / "server.db";
    SqliteUploadSessionRepository repository(database_path, migrations_directory);
    Require(repository.Initialize().ok(), "additional migration must succeed");
    Require(ReadAppliedMigrationVersion(database_path) == 2, "additional migration must be recorded");
    Require(repository.Initialize().ok(), "reapplying migrations must be idempotent");
    Require(ReadAppliedMigrationVersion(database_path) == 2, "migration version must not be duplicated");
}

void TestStateTransitionCompareAndSet() {
    TemporaryDirectory temporary_directory;
    SqliteUploadSessionRepository repository(temporary_directory.path / "server.db");
    Require(repository.Initialize().ok(), "migrations must succeed");
    AddDirectory(repository);
    Require(repository.CreateSession(NewSession("session-1"), {}).ok(), "session must be created");

    Require(repository.TransitionSession("session-1", UploadSessionState::kCreated,
                                         UploadSessionState::kUploading, 2'000).ok(),
            "created -> uploading must succeed");
    const auto stale = repository.TransitionSession("session-1", UploadSessionState::kCreated,
                                                     UploadSessionState::kUploading, 3'000);
    Require(stale.code == RepositoryError::kConflict, "stale transition must be a conflict");
    const auto invalid = repository.TransitionSession("session-1", UploadSessionState::kUploading,
                                                       UploadSessionState::kCommitted, 3'000);
    Require(invalid.code == RepositoryError::kInvalidArgument, "illegal transition must be rejected");
}

void TestCreateSessionValidationAndExpiry() {
    TemporaryDirectory temporary_directory;
    SqliteUploadSessionRepository repository(temporary_directory.path / "server.db");
    Require(repository.Initialize().ok(), "migrations must succeed");
    AddDirectory(repository);

    auto stale = NewSession("stale");
    stale.base_revision = 2;
    const auto stale_status = repository.CreateSession(stale, {});
    Require(stale_status.code == RepositoryError::kConflict, "stale base revision must be rejected");
    Require(!repository.GetSession(stale.id).has_value(), "rejected session must not be written");

    auto expiring = NewSession("expiring", 1'000);
    expiring.expires_at_ms = 1'500;
    Require(repository.CreateSession(expiring, {}).ok(), "expiring session must be created");
    Require(repository.ExpireSessions(1'499) == 0, "lease must not expire too early");
    Require(repository.ExpireSessions(1'500) == 1, "expired session must be updated");
    const auto stored = repository.GetSession(expiring.id);
    Require(stored && stored->state == UploadSessionState::kExpired, "session state must be expired");
}

}  // namespace

int main() {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"schema_and_persistence", TestSchemaAndPersistence},
        {"additional_migration_is_applied_once", TestAdditionalMigrationIsAppliedOnce},
        {"state_transition_compare_and_set", TestStateTransitionCompareAndSet},
        {"create_session_validation_and_expiry", TestCreateSessionValidationAndExpiry},
    };
    for (const auto& [name, test] : tests) {
        try {
            test();
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& error) {
            std::cerr << "[FAIL] " << name << ": " << error.what() << '\n';
            return 1;
        }
    }
    return 0;
}
