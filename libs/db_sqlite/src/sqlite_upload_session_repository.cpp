#include "synxpo/db_sqlite/sqlite_upload_session_repository.h"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <limits>
#include <set>
#include <system_error>

#include <sqlite3.h>

namespace synxpo::db_sqlite {
namespace {

using db::RepositoryError;
using db::RepositoryStatus;
using domain::Directory;
using domain::IsTransitionAllowed;
using domain::ParseUploadFileOperation;
using domain::ParseUploadFileState;
using domain::ParseUploadSessionState;
using domain::ToString;
using domain::UploadFileOperation;
using domain::UploadFileState;
using domain::UploadSession;
using domain::UploadSessionFile;
using domain::UploadSessionState;

constexpr auto kSqliteOk = SQLITE_OK;

RepositoryStatus StorageError(sqlite3* db, const std::string& context) {
    return {RepositoryError::kStorage, context + ": " + sqlite3_errmsg(db)};
}

RepositoryStatus ConstraintOrStorageError(sqlite3* db, const std::string& context) {
    if (sqlite3_extended_errcode(db) == SQLITE_CONSTRAINT ||
        (sqlite3_extended_errcode(db) & 0xff) == SQLITE_CONSTRAINT) {
        return {RepositoryError::kConflict, context + ": " + sqlite3_errmsg(db)};
    }
    return StorageError(db, context);
}

class Statement {
public:
    Statement(sqlite3* db, const char* sql) : db_(db) {
        if (sqlite3_prepare_v2(db, sql, -1, &statement_, nullptr) != kSqliteOk) {
            error_ = StorageError(db, "prepare statement");
        }
    }

    ~Statement() { sqlite3_finalize(statement_); }

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    [[nodiscard]] bool valid() const { return statement_ != nullptr; }
    [[nodiscard]] sqlite3_stmt* get() const { return statement_; }
    [[nodiscard]] const RepositoryStatus& error() const { return error_; }

private:
    sqlite3* db_;
    sqlite3_stmt* statement_ = nullptr;
    RepositoryStatus error_;
};

bool BindText(sqlite3_stmt* statement, int index, const std::string& value) {
    return sqlite3_bind_text(statement, index, value.c_str(), -1, SQLITE_TRANSIENT) == kSqliteOk;
}

bool BindOptionalText(sqlite3_stmt* statement, int index, const std::optional<std::string>& value) {
    return value ? BindText(statement, index, *value) : sqlite3_bind_null(statement, index) == kSqliteOk;
}

bool BindUInt(sqlite3_stmt* statement, int index, std::uint64_t value) {
    if (value > static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max())) return false;
    return sqlite3_bind_int64(statement, index, static_cast<sqlite3_int64>(value)) == kSqliteOk;
}

bool BindOptionalUInt(sqlite3_stmt* statement, int index, const std::optional<std::uint64_t>& value) {
    return value ? BindUInt(statement, index, *value) : sqlite3_bind_null(statement, index) == kSqliteOk;
}

std::string ColumnText(sqlite3_stmt* statement, int column) {
    const auto* value = sqlite3_column_text(statement, column);
    return value == nullptr ? "" : reinterpret_cast<const char*>(value);
}

std::optional<std::string> OptionalColumnText(sqlite3_stmt* statement, int column) {
    if (sqlite3_column_type(statement, column) == SQLITE_NULL) return std::nullopt;
    return ColumnText(statement, column);
}

std::optional<std::uint64_t> OptionalColumnUInt(sqlite3_stmt* statement, int column) {
    if (sqlite3_column_type(statement, column) == SQLITE_NULL) return std::nullopt;
    const auto value = sqlite3_column_int64(statement, column);
    if (value < 0) return std::nullopt;
    return static_cast<std::uint64_t>(value);
}

std::optional<UploadSession> ReadSession(sqlite3_stmt* statement) {
    const auto state = ParseUploadSessionState(ColumnText(statement, 4));
    if (!state) return std::nullopt;
    const auto base_revision = sqlite3_column_int64(statement, 3);
    if (base_revision < 0) return std::nullopt;
    UploadSession session;
    session.id = ColumnText(statement, 0);
    session.directory_id = ColumnText(statement, 1);
    session.owner_id = ColumnText(statement, 2);
    session.base_revision = static_cast<std::uint64_t>(base_revision);
    session.state = *state;
    session.manifest_hash = OptionalColumnText(statement, 5);
    session.idempotency_key = OptionalColumnText(statement, 6);
    session.created_at_ms = sqlite3_column_int64(statement, 7);
    session.updated_at_ms = sqlite3_column_int64(statement, 8);
    session.expires_at_ms = sqlite3_column_int64(statement, 9);
    session.committed_revision = OptionalColumnUInt(statement, 10);
    session.error_code = OptionalColumnText(statement, 11);
    session.error_message = OptionalColumnText(statement, 12);
    return session;
}

std::optional<UploadSessionFile> ReadSessionFile(sqlite3_stmt* statement) {
    const auto operation = ParseUploadFileOperation(ColumnText(statement, 3));
    const auto state = ParseUploadFileState(ColumnText(statement, 9));
    if (!operation || !state) return std::nullopt;
    UploadSessionFile file;
    file.session_id = ColumnText(statement, 0);
    file.file_id = OptionalColumnText(statement, 1);
    file.path = ColumnText(statement, 2);
    file.operation = *operation;
    file.expected_version = OptionalColumnUInt(statement, 4);
    file.content_hash = OptionalColumnText(statement, 5);
    file.size = OptionalColumnUInt(statement, 6);
    file.staging_path = OptionalColumnText(statement, 7);
    const auto received_bytes = sqlite3_column_int64(statement, 8);
    if (received_bytes < 0) return std::nullopt;
    file.received_bytes = static_cast<std::uint64_t>(received_bytes);
    file.state = *state;
    return file;
}

}  // namespace

using db::RepositoryError;
using db::RepositoryStatus;
using namespace domain;

SqliteUploadSessionRepository::SqliteUploadSessionRepository(
    std::filesystem::path database_path,
    std::filesystem::path migrations_directory)
    : database_path_(std::move(database_path)),
      migrations_directory_(std::move(migrations_directory)) {}

SqliteUploadSessionRepository::~SqliteUploadSessionRepository() {
    if (db_ != nullptr) sqlite3_close(db_);
}

std::filesystem::path SqliteUploadSessionRepository::DefaultMigrationsDirectory() {
    return SYNXPO_SQLITE_MIGRATIONS_DIRECTORY;
}

RepositoryStatus SqliteUploadSessionRepository::Initialize() {
    auto status = Open();
    if (!status.ok()) return status;
    return ApplyMigrations();
}

RepositoryStatus SqliteUploadSessionRepository::Open() {
    if (db_ != nullptr) return RepositoryStatus::Ok();
    std::error_code error;
    const auto parent = database_path_.parent_path();
    if (!parent.empty()) std::filesystem::create_directories(parent, error);
    if (error) return {RepositoryError::kStorage, "create database directory: " + error.message()};

    if (sqlite3_open_v2(database_path_.string().c_str(), &db_,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                        nullptr) != kSqliteOk) {
        const auto status = StorageError(db_, "open database");
        sqlite3_close(db_);
        db_ = nullptr;
        return status;
    }
    sqlite3_busy_timeout(db_, 5000);
    return Execute("PRAGMA foreign_keys = ON;");
}

RepositoryStatus SqliteUploadSessionRepository::Execute(const std::string& sql) const {
    char* error_message = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error_message) != kSqliteOk) {
        std::string message = error_message == nullptr ? sqlite3_errmsg(db_) : error_message;
        sqlite3_free(error_message);
        return {RepositoryError::kStorage, message};
    }
    return RepositoryStatus::Ok();
}

RepositoryStatus SqliteUploadSessionRepository::ApplyMigrations() {
    auto status = Execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL);");
    if (!status.ok()) return status;

    std::error_code error;
    if (!std::filesystem::is_directory(migrations_directory_, error) || error) {
        return {RepositoryError::kStorage, "migration directory is unavailable: " + migrations_directory_.string()};
    }

    struct Migration { int version; std::filesystem::path path; };
    std::vector<Migration> migrations;
    for (const auto& entry : std::filesystem::directory_iterator(migrations_directory_, error)) {
        if (error) return {RepositoryError::kStorage, "read migration directory: " + error.message()};
        const auto filename = entry.path().filename().string();
        const auto separator = filename.find('_');
        if (!entry.is_regular_file() || entry.path().extension() != ".sql" || separator == std::string::npos) continue;
        int version = 0;
        const auto [end, parse_error] = std::from_chars(filename.data(), filename.data() + separator, version);
        if (parse_error != std::errc{} || end != filename.data() + separator || version <= 0) {
            return {RepositoryError::kStorage, "invalid migration filename: " + filename};
        }
        migrations.push_back({version, entry.path()});
    }
    std::sort(migrations.begin(), migrations.end(), [](const auto& left, const auto& right) {
        return left.version < right.version;
    });
    for (std::size_t index = 1; index < migrations.size(); ++index) {
        if (migrations[index - 1].version == migrations[index].version) {
            return {RepositoryError::kStorage, "duplicate migration version"};
        }
    }

    Statement applied_query(db_, "SELECT version FROM schema_migrations ORDER BY version;");
    if (!applied_query.valid()) return applied_query.error();
    std::set<int> applied;
    while (sqlite3_step(applied_query.get()) == SQLITE_ROW) applied.insert(sqlite3_column_int(applied_query.get(), 0));
    if (sqlite3_errcode(db_) != SQLITE_OK && sqlite3_errcode(db_) != SQLITE_DONE) {
        return StorageError(db_, "read applied migrations");
    }
    const auto latest_known = migrations.empty() ? 0 : migrations.back().version;
    if (!applied.empty() && *applied.rbegin() > latest_known) {
        return {RepositoryError::kStorage, "database schema is newer than this binary"};
    }

    for (const auto& migration : migrations) {
        if (applied.contains(migration.version)) continue;
        std::ifstream input(migration.path);
        const std::string sql((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
        if (!input.good() && !input.eof()) return {RepositoryError::kStorage, "read migration: " + migration.path.string()};
        if (sql.empty()) return {RepositoryError::kStorage, "migration is empty: " + migration.path.string()};
        status = Execute("BEGIN IMMEDIATE;");
        if (!status.ok()) return status;
        const auto rollback = [this]() { Execute("ROLLBACK;"); };
        status = Execute(sql);
        if (!status.ok()) { rollback(); return {status.code, migration.path.filename().string() + ": " + status.message}; }
        Statement record(db_, "INSERT INTO schema_migrations(version, name) VALUES (?, ?);");
        if (!record.valid() || sqlite3_bind_int(record.get(), 1, migration.version) != kSqliteOk ||
            !BindText(record.get(), 2, migration.path.filename().string()) || sqlite3_step(record.get()) != SQLITE_DONE) {
            rollback();
            return record.valid() ? StorageError(db_, "record migration") : record.error();
        }
        status = Execute("COMMIT;");
        if (!status.ok()) { rollback(); return status; }
    }
    return RepositoryStatus::Ok();
}

RepositoryStatus SqliteUploadSessionRepository::CreateDirectory(const Directory& directory) {
    if (directory.id.empty()) return {RepositoryError::kInvalidArgument, "directory id is required"};
    Statement statement(db_, "INSERT INTO directories(id, current_revision, created_at_ms) VALUES (?, ?, ?);");
    if (!statement.valid()) return statement.error();
    if (!BindText(statement.get(), 1, directory.id) || !BindUInt(statement.get(), 2, directory.current_revision) ||
        sqlite3_bind_int64(statement.get(), 3, directory.created_at_ms) != kSqliteOk) {
        return StorageError(db_, "bind directory");
    }
    if (sqlite3_step(statement.get()) != SQLITE_DONE) return ConstraintOrStorageError(db_, "create directory");
    return RepositoryStatus::Ok();
}

std::optional<Directory> SqliteUploadSessionRepository::GetDirectory(const std::string& directory_id) const {
    Statement statement(db_, "SELECT id, current_revision, created_at_ms FROM directories WHERE id = ?;");
    if (!statement.valid() || !BindText(statement.get(), 1, directory_id) || sqlite3_step(statement.get()) != SQLITE_ROW) return std::nullopt;
    const auto revision = sqlite3_column_int64(statement.get(), 1);
    if (revision < 0) return std::nullopt;
    return Directory{ColumnText(statement.get(), 0), static_cast<std::uint64_t>(revision), sqlite3_column_int64(statement.get(), 2)};
}

RepositoryStatus SqliteUploadSessionRepository::CreateSession(
    const UploadSession& session, const std::vector<UploadSessionFile>& files) {
    if (session.id.empty() || session.directory_id.empty() || session.owner_id.empty()) {
        return {RepositoryError::kInvalidArgument, "session id, directory id and owner id are required"};
    }
    if (session.state != UploadSessionState::kCreated || session.expires_at_ms <= session.created_at_ms) {
        return {RepositoryError::kInvalidArgument, "a new session must be created with a future expiry"};
    }
    std::set<std::string> paths;
    for (const auto& file : files) {
        if (file.session_id != session.id || file.path.empty() || !paths.insert(file.path).second) {
            return {RepositoryError::kInvalidArgument, "manifest has an invalid session id, path, or duplicate path"};
        }
    }

    auto status = Execute("BEGIN IMMEDIATE;");
    if (!status.ok()) return status;
    const auto rollback = [this]() { Execute("ROLLBACK;"); };

    const auto directory = GetDirectory(session.directory_id);
    if (!directory) { rollback(); return {RepositoryError::kNotFound, "directory does not exist"}; }
    if (directory->current_revision != session.base_revision) {
        rollback(); return {RepositoryError::kConflict, "base revision is stale"};
    }

    Statement session_insert(db_, R"sql(
        INSERT INTO upload_sessions(
            id, directory_id, owner_id, base_revision, state, manifest_hash, idempotency_key,
            created_at_ms, updated_at_ms, expires_at_ms, committed_revision, error_code, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    )sql");
    if (!session_insert.valid()) { rollback(); return session_insert.error(); }
    if (!BindText(session_insert.get(), 1, session.id) || !BindText(session_insert.get(), 2, session.directory_id) ||
        !BindText(session_insert.get(), 3, session.owner_id) || !BindUInt(session_insert.get(), 4, session.base_revision) ||
        !BindText(session_insert.get(), 5, ToString(session.state)) || !BindOptionalText(session_insert.get(), 6, session.manifest_hash) ||
        !BindOptionalText(session_insert.get(), 7, session.idempotency_key) ||
        sqlite3_bind_int64(session_insert.get(), 8, session.created_at_ms) != kSqliteOk ||
        sqlite3_bind_int64(session_insert.get(), 9, session.updated_at_ms) != kSqliteOk ||
        sqlite3_bind_int64(session_insert.get(), 10, session.expires_at_ms) != kSqliteOk ||
        !BindOptionalUInt(session_insert.get(), 11, session.committed_revision) ||
        !BindOptionalText(session_insert.get(), 12, session.error_code) ||
        !BindOptionalText(session_insert.get(), 13, session.error_message)) {
        rollback(); return StorageError(db_, "bind session");
    }
    if (sqlite3_step(session_insert.get()) != SQLITE_DONE) {
        status = ConstraintOrStorageError(db_, "create session"); rollback(); return status;
    }

    Statement file_insert(db_, R"sql(
        INSERT INTO upload_session_files(
            session_id, file_id, path, operation, expected_version, content_hash, size,
            staging_path, received_bytes, state)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    )sql");
    if (!file_insert.valid()) { rollback(); return file_insert.error(); }
    for (const auto& file : files) {
        sqlite3_reset(file_insert.get());
        sqlite3_clear_bindings(file_insert.get());
        if (!BindText(file_insert.get(), 1, file.session_id) || !BindOptionalText(file_insert.get(), 2, file.file_id) ||
            !BindText(file_insert.get(), 3, file.path) || !BindText(file_insert.get(), 4, ToString(file.operation)) ||
            !BindOptionalUInt(file_insert.get(), 5, file.expected_version) || !BindOptionalText(file_insert.get(), 6, file.content_hash) ||
            !BindOptionalUInt(file_insert.get(), 7, file.size) || !BindOptionalText(file_insert.get(), 8, file.staging_path) ||
            !BindUInt(file_insert.get(), 9, file.received_bytes) || !BindText(file_insert.get(), 10, ToString(file.state))) {
            rollback(); return StorageError(db_, "bind session file");
        }
        if (sqlite3_step(file_insert.get()) != SQLITE_DONE) {
            status = ConstraintOrStorageError(db_, "create session file"); rollback(); return status;
        }
    }
    status = Execute("COMMIT;");
    if (!status.ok()) { rollback(); return status; }
    return RepositoryStatus::Ok();
}

std::optional<UploadSession> SqliteUploadSessionRepository::GetSession(const std::string& session_id) const {
    Statement statement(db_, R"sql(
        SELECT id, directory_id, owner_id, base_revision, state, manifest_hash, idempotency_key,
               created_at_ms, updated_at_ms, expires_at_ms, committed_revision, error_code, error_message
        FROM upload_sessions WHERE id = ?;
    )sql");
    if (!statement.valid() || !BindText(statement.get(), 1, session_id) || sqlite3_step(statement.get()) != SQLITE_ROW) return std::nullopt;
    return ReadSession(statement.get());
}

std::vector<UploadSessionFile> SqliteUploadSessionRepository::ListSessionFiles(const std::string& session_id) const {
    std::vector<UploadSessionFile> files;
    Statement statement(db_, R"sql(
        SELECT session_id, file_id, path, operation, expected_version, content_hash, size,
               staging_path, received_bytes, state
        FROM upload_session_files WHERE session_id = ? ORDER BY path;
    )sql");
    if (!statement.valid() || !BindText(statement.get(), 1, session_id)) return files;
    while (sqlite3_step(statement.get()) == SQLITE_ROW) {
        if (const auto file = ReadSessionFile(statement.get())) files.push_back(*file);
    }
    return files;
}

RepositoryStatus SqliteUploadSessionRepository::TransitionSession(
    const std::string& session_id, UploadSessionState expected_state,
    UploadSessionState next_state, std::int64_t updated_at_ms) {
    if (!IsTransitionAllowed(expected_state, next_state)) {
        return {RepositoryError::kInvalidArgument, "invalid upload session state transition"};
    }
    Statement statement(db_, R"sql(
        UPDATE upload_sessions SET state = ?, updated_at_ms = ? WHERE id = ? AND state = ?;
    )sql");
    if (!statement.valid()) return statement.error();
    if (!BindText(statement.get(), 1, ToString(next_state)) ||
        sqlite3_bind_int64(statement.get(), 2, updated_at_ms) != kSqliteOk ||
        !BindText(statement.get(), 3, session_id) || !BindText(statement.get(), 4, ToString(expected_state))) {
        return StorageError(db_, "bind session transition");
    }
    if (sqlite3_step(statement.get()) != SQLITE_DONE) return StorageError(db_, "transition session");
    if (sqlite3_changes(db_) == 1) return RepositoryStatus::Ok();
    return GetSession(session_id)
        ? RepositoryStatus{RepositoryError::kConflict, "session is not in the expected state"}
        : RepositoryStatus{RepositoryError::kNotFound, "session does not exist"};
}

std::uint64_t SqliteUploadSessionRepository::ExpireSessions(std::int64_t now_ms) {
    Statement statement(db_, R"sql(
        UPDATE upload_sessions SET state = 'expired', updated_at_ms = ?
        WHERE expires_at_ms <= ? AND state NOT IN ('committed', 'aborted', 'expired');
    )sql");
    if (!statement.valid() || sqlite3_bind_int64(statement.get(), 1, now_ms) != kSqliteOk ||
        sqlite3_bind_int64(statement.get(), 2, now_ms) != kSqliteOk || sqlite3_step(statement.get()) != SQLITE_DONE) {
        return 0;
    }
    return static_cast<std::uint64_t>(sqlite3_changes(db_));
}

}  // namespace synxpo::db_sqlite
