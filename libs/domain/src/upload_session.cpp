#include "synxpo/domain/upload_session.h"

namespace synxpo::domain {

const char* ToString(UploadSessionState state) {
    switch (state) {
        case UploadSessionState::kCreated: return "created";
        case UploadSessionState::kUploading: return "uploading";
        case UploadSessionState::kValidating: return "validating";
        case UploadSessionState::kCommitting: return "committing";
        case UploadSessionState::kCommitted: return "committed";
        case UploadSessionState::kAborted: return "aborted";
        case UploadSessionState::kExpired: return "expired";
    }
    return "unknown";
}

const char* ToString(UploadFileOperation operation) {
    switch (operation) {
        case UploadFileOperation::kCreate: return "create";
        case UploadFileOperation::kUpdate: return "update";
        case UploadFileOperation::kDelete: return "delete";
        case UploadFileOperation::kRename: return "rename";
    }
    return "unknown";
}

const char* ToString(UploadFileState state) {
    switch (state) {
        case UploadFileState::kPending: return "pending";
        case UploadFileState::kUploading: return "uploading";
        case UploadFileState::kComplete: return "complete";
        case UploadFileState::kFailed: return "failed";
    }
    return "unknown";
}

std::optional<UploadSessionState> ParseUploadSessionState(const std::string& value) {
    for (const auto state : {UploadSessionState::kCreated, UploadSessionState::kUploading,
                             UploadSessionState::kValidating, UploadSessionState::kCommitting,
                             UploadSessionState::kCommitted, UploadSessionState::kAborted,
                             UploadSessionState::kExpired}) {
        if (value == ToString(state)) return state;
    }
    return std::nullopt;
}

std::optional<UploadFileOperation> ParseUploadFileOperation(const std::string& value) {
    for (const auto operation : {UploadFileOperation::kCreate, UploadFileOperation::kUpdate,
                                 UploadFileOperation::kDelete, UploadFileOperation::kRename}) {
        if (value == ToString(operation)) return operation;
    }
    return std::nullopt;
}

std::optional<UploadFileState> ParseUploadFileState(const std::string& value) {
    for (const auto state : {UploadFileState::kPending, UploadFileState::kUploading,
                             UploadFileState::kComplete, UploadFileState::kFailed}) {
        if (value == ToString(state)) return state;
    }
    return std::nullopt;
}

bool IsTerminal(UploadSessionState state) {
    return state == UploadSessionState::kCommitted || state == UploadSessionState::kAborted ||
           state == UploadSessionState::kExpired;
}

bool IsTransitionAllowed(UploadSessionState from, UploadSessionState to) {
    if (from == to || IsTerminal(from)) return false;
    switch (from) {
        case UploadSessionState::kCreated:
            return to == UploadSessionState::kUploading || to == UploadSessionState::kAborted ||
                   to == UploadSessionState::kExpired;
        case UploadSessionState::kUploading:
            return to == UploadSessionState::kValidating || to == UploadSessionState::kAborted ||
                   to == UploadSessionState::kExpired;
        case UploadSessionState::kValidating:
            return to == UploadSessionState::kCommitting || to == UploadSessionState::kAborted ||
                   to == UploadSessionState::kExpired;
        case UploadSessionState::kCommitting:
            return to == UploadSessionState::kCommitted || to == UploadSessionState::kAborted;
        case UploadSessionState::kCommitted:
        case UploadSessionState::kAborted:
        case UploadSessionState::kExpired:
            return false;
    }
    return false;
}

}  // namespace synxpo::domain
