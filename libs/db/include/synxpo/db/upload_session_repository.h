#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "synxpo/domain/upload_session.h"

namespace synxpo::db {

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

// Contract used by application code. It intentionally contains no storage types.
class UploadSessionRepository {
public:
    virtual ~UploadSessionRepository() = default;

    virtual RepositoryStatus CreateDirectory(const domain::Directory& directory) = 0;
    virtual std::optional<domain::Directory> GetDirectory(const std::string& directory_id) const = 0;

    virtual RepositoryStatus CreateSession(
        const domain::UploadSession& session,
        const std::vector<domain::UploadSessionFile>& files) = 0;
    virtual std::optional<domain::UploadSession> GetSession(const std::string& session_id) const = 0;
    virtual std::vector<domain::UploadSessionFile> ListSessionFiles(const std::string& session_id) const = 0;

    virtual RepositoryStatus TransitionSession(
        const std::string& session_id,
        domain::UploadSessionState expected_state,
        domain::UploadSessionState next_state,
        std::int64_t updated_at_ms) = 0;
    virtual std::uint64_t ExpireSessions(std::int64_t now_ms) = 0;
};

}  // namespace synxpo::db
