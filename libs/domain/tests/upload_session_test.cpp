#include <iostream>
#include <stdexcept>

#include "synxpo/domain/upload_session.h"

namespace {

void Require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

void TestStateTransitions() {
    using synxpo::domain::IsTransitionAllowed;
    using synxpo::domain::UploadSessionState;

    Require(IsTransitionAllowed(UploadSessionState::kCreated, UploadSessionState::kUploading),
            "created session must become uploading");
    Require(!IsTransitionAllowed(UploadSessionState::kCreated, UploadSessionState::kCommitted),
            "created session must not be committed directly");
    Require(!IsTransitionAllowed(UploadSessionState::kCommitted, UploadSessionState::kUploading),
            "terminal session must not transition");
}

}  // namespace

int main() {
    try {
        TestStateTransitions();
        std::cout << "[PASS] state_transitions\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] state_transitions: " << error.what() << '\n';
        return 1;
    }
}
