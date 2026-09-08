#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace synxpo::domain {

enum class UploadSessionState {
    kCreated,
    kUploading,
    kValidating,
    kCommitting,
    kCommitted,
    kAborted,
    kExpired,
};

enum class UploadFileOperation {
    kCreate,
    kUpdate,
    kDelete,
    kRename,
};

enum class UploadFileState {
    kPending,
    kUploading,
    kComplete,
    kFailed,
};

struct Directory {
    std::string id;
    std::uint64_t current_revision = 0;
    std::int64_t created_at_ms = 0;
};

struct UploadSession {
    std::string id;
    std::string directory_id;
    std::string owner_id;
    std::uint64_t base_revision = 0;
    UploadSessionState state = UploadSessionState::kCreated;
    std::optional<std::string> manifest_hash;
    std::optional<std::string> idempotency_key;
    std::int64_t created_at_ms = 0;
    std::int64_t updated_at_ms = 0;
    std::int64_t expires_at_ms = 0;
    std::optional<std::uint64_t> committed_revision;
    std::optional<std::string> error_code;
    std::optional<std::string> error_message;
};

struct UploadSessionFile {
    std::string session_id;
    std::optional<std::string> file_id;
    std::string path;
    UploadFileOperation operation = UploadFileOperation::kCreate;
    std::optional<std::uint64_t> expected_version;
    std::optional<std::string> content_hash;
    std::optional<std::uint64_t> size;
    std::optional<std::string> staging_path;
    std::uint64_t received_bytes = 0;
    UploadFileState state = UploadFileState::kPending;
};

const char* ToString(UploadSessionState state);
const char* ToString(UploadFileOperation operation);
const char* ToString(UploadFileState state);

std::optional<UploadSessionState> ParseUploadSessionState(const std::string& value);
std::optional<UploadFileOperation> ParseUploadFileOperation(const std::string& value);
std::optional<UploadFileState> ParseUploadFileState(const std::string& value);

bool IsTerminal(UploadSessionState state);
bool IsTransitionAllowed(UploadSessionState from, UploadSessionState to);

}  // namespace synxpo::domain
