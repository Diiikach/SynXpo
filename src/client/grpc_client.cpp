#include "synxpo/client/grpc_client.h"

#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>

#include <absl/log/log.h>
#include <absl/log/vlog_is_on.h>
#include <absl/strings/str_cat.h>

namespace {
// Helper to get message type name for logging
std::string GetClientMessageType(const synxpo::ClientMessage& msg) {
    if (msg.has_directory_create()) return "DIRECTORY_CREATE";
    if (msg.has_directory_subscribe()) return "DIRECTORY_SUBSCRIBE";
    if (msg.has_directory_unsubscribe()) return "DIRECTORY_UNSUBSCRIBE";
    if (msg.has_request_version()) return "REQUEST_VERSION";
    if (msg.has_ask_version_increase()) return "ASK_VERSION_INCREASE";
    if (msg.has_file_write()) return "FILE_WRITE";
    if (msg.has_file_write_end()) return "FILE_WRITE_END";
    if (msg.has_request_file_content()) return "REQUEST_FILE_CONTENT";
    return "UNKNOWN";
}

std::string GetServerMessageType(const synxpo::ServerMessage& msg) {
    if (msg.has_ok_directory_created()) return "OK_DIRECTORY_CREATED";
    if (msg.has_ok_subscribed()) return "OK_SUBSCRIBED";
    if (msg.has_ok_unsubscribed()) return "OK_UNSUBSCRIBED";
    if (msg.has_check_version()) return "CHECK_VERSION";
    if (msg.has_version_increase_allow()) return "VERSION_INCREASE_ALLOW";
    if (msg.has_version_increase_deny()) return "VERSION_INCREASE_DENY";
    if (msg.has_version_increased()) return "VERSION_INCREASED";
    if (msg.has_file_content_request_allow()) return "FILE_CONTENT_REQUEST_ALLOW";
    if (msg.has_file_content_request_deny()) return "FILE_CONTENT_REQUEST_DENY";
    if (msg.has_file_write()) return "FILE_WRITE";
    if (msg.has_file_write_end()) return "FILE_WRITE_END";
    if (msg.has_error()) return "ERROR";
    return "UNKNOWN";
}
}  // namespace

namespace synxpo {

std::string GRPCClient::GenerateUuid() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;
    
    // Generate two 64-bit random numbers for 128-bit UUID
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    // Set version (4) and variant (RFC 4122)
    high = (high & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    low = (low & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
    
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << (high >> 32)
        << '-'
        << std::setw(4) << ((high >> 16) & 0xFFFF)
        << '-'
        << std::setw(4) << (high & 0xFFFF)
        << '-'
        << std::setw(4) << (low >> 48)
        << '-'
        << std::setw(12) << (low & 0xFFFFFFFFFFFFULL);
    
    return oss.str();
}

GRPCClient::GRPCClient(const std::string& server_address)
    : server_address_(server_address) {}

GRPCClient::~GRPCClient() {
    Disconnect();
}

absl::Status GRPCClient::Connect() {
    if (connected_) {
        return absl::OkStatus();
    }

    channel_ = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    if (!channel_) {
        return absl::InternalError("Failed to create gRPC channel");
    }

    stub_ = SyncService::NewStub(channel_);
    if (!stub_) {
        channel_.reset();
        return absl::InternalError("Failed to create gRPC stub");
    }

    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    if (!channel_->WaitForConnected(deadline)) {
        stub_.reset();
        channel_.reset();
        return absl::UnavailableError(
            absl::StrCat("Failed to connect to server: ", server_address_));
    }

    stream_context_ = std::make_unique<grpc::ClientContext>();
    stream_ = stub_->Stream(stream_context_.get());
    if (!stream_) {
        stub_.reset();
        channel_.reset();
        return absl::InternalError("Failed to create bidirectional stream");
    }

    connected_ = true;
    return absl::OkStatus();
}

void GRPCClient::Disconnect() {
    if (!connected_) {
        return;
    }

    if (stream_context_) {
        stream_context_->TryCancel();
    }
    
    StopReceiving();
    
    if (stream_) {
        stream_->WritesDone();
        stream_->Finish();
        stream_.reset();
    }
    stream_context_.reset();
    
    stub_.reset();
    channel_.reset();
    connected_ = false;
}

bool GRPCClient::IsConnected() const {
    return connected_;
}

absl::Status GRPCClient::SendMessage(const ClientMessage& message) {
    if (!connected_ || !stream_) {
        return absl::FailedPreconditionError("Not connected to server");
    }

    std::lock_guard<std::mutex> lock(stream_mutex_);
    
    LOG(INFO) << "[gRPC] --> " << GetClientMessageType(message);
    if (message.has_request_id()) {
        LOG(INFO) << "[gRPC]     request_id=" << message.request_id();
    }
    VLOG(1) << "[gRPC] --> Content:\n" << message.DebugString();
    
    if (!stream_->Write(message)) {
        LOG(WARNING) << "[gRPC] Failed to write message";
        return absl::UnavailableError("Failed to write message to stream");
    }

    return absl::OkStatus();
}

absl::StatusOr<ServerMessage> GRPCClient::SendMessageWithResponse(
    ClientMessage& message,
    std::chrono::milliseconds timeout) {
    
    if (!connected_ || !stream_) {
        return absl::FailedPreconditionError("Not connected to server");
    }
    
    std::string request_id = GenerateUuid();
    message.set_request_id(request_id);
    
    auto send_status = SendMessage(message);
    if (!send_status.ok()) {
        return send_status;
    }
    
    auto predicate = [request_id](const ServerMessage& msg) {
        return msg.has_request_id() && msg.request_id() == request_id;
    };
    
    return WaitForMessage(predicate, timeout);
}

void GRPCClient::SetMessageCallback(ServerMessageCallback callback) {
    std::lock_guard<std::mutex> lock(waiters_mutex_);
    message_callback_ = std::move(callback);
}

void GRPCClient::StartReceiving() {
    if (receiving_) {
        return;
    }

    if (!connected_) {
        return;
    }

    should_stop_ = false;
    receiving_ = true;
    receive_thread_ = std::thread([this]() { ReceiveLoop(); });
    callback_worker_ = std::thread([this]() { CallbackWorkerLoop(); });
}

void GRPCClient::StopReceiving() {
    if (!receiving_) {
        return;
    }

    should_stop_ = true;

    {
        std::lock_guard<std::mutex> lock(waiters_mutex_);
        for (auto& waiter : waiters_) {
            std::lock_guard<std::mutex> waiter_lock(waiter->mutex);
            waiter->cancelled = true;
            waiter->done = true;
            waiter->cv.notify_one();
        }
        waiters_.clear();
    }

    callback_cv_.notify_one();
    if (callback_worker_.joinable()) {
        callback_worker_.join();
    }

    if (receive_thread_.joinable()) {
        receive_thread_.join();
    }

    receiving_ = false;
}

bool GRPCClient::IsReceiving() const {
    return receiving_;
}

absl::StatusOr<ServerMessage> GRPCClient::WaitForMessage(
    MessagePredicate predicate,
    std::chrono::milliseconds timeout) {
    
    if (!receiving_) {
        return absl::FailedPreconditionError("Message receiving is not started");
    }

    auto waiter = std::make_shared<Waiter>();
    waiter->predicate = std::move(predicate);

    {
        std::lock_guard<std::mutex> lock(waiters_mutex_);
        waiters_.push_back(waiter);
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lock(waiter->mutex);
    
    while (!waiter->done) {
        if (waiter->cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            std::lock_guard<std::mutex> waiters_lock(waiters_mutex_);
            waiters_.erase(
                std::remove(waiters_.begin(), waiters_.end(), waiter),
                waiters_.end());
            return absl::DeadlineExceededError("Timeout waiting for message");
        }
    }

    if (waiter->cancelled) {
        return absl::CancelledError("Receiving stopped");
    }

    if (!waiter->result) {
        return absl::InternalError("Waiter completed without result");
    }

    return std::move(*waiter->result);
}

void GRPCClient::ReceiveLoop() {
    while (!should_stop_ && connected_ && stream_) {
        ServerMessage message;
        
        if (stream_->Read(&message)) {
            LOG(INFO) << "[gRPC] <-- " << GetServerMessageType(message);
            if (message.has_request_id()) {
                LOG(INFO) << "[gRPC]     request_id=" << message.request_id();
            }
            if (message.has_error()) {
                LOG(WARNING) << "[gRPC]     error: " << message.error().message();
            }
            VLOG(1) << "[gRPC] <-- Content:\n" << message.DebugString();
            ProcessMessage(message);
        } else {
            // Stream closed or error
            LOG(INFO) << "[gRPC] Stream closed";
            break;
        }
    }
}

void GRPCClient::ProcessMessage(const ServerMessage& message) {
    {
        std::lock_guard<std::mutex> lock(waiters_mutex_);
        
        for (auto it = waiters_.begin(); it != waiters_.end(); ) {
            auto& waiter = *it;
            
            if (waiter->predicate(message)) {
                std::lock_guard<std::mutex> waiter_lock(waiter->mutex);
                waiter->result = message;
                waiter->done = true;
                waiter->cv.notify_one();
                
                it = waiters_.erase(it);
                return;
            } else {
                ++it;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        callback_queue_.push(message);
    }
    callback_cv_.notify_one();
}

void GRPCClient::CallbackWorkerLoop() {
    while (!should_stop_) {
        std::unique_lock<std::mutex> lock(callback_mutex_);
        callback_cv_.wait(lock, [this] { 
            return !callback_queue_.empty() || should_stop_; 
        });
        
        if (should_stop_) {
            break;
        }
        
        auto message = std::move(callback_queue_.front());
        callback_queue_.pop();
        lock.unlock();
        
        if (message_callback_) {
            message_callback_(message);
        }
    }
}

}  // namespace synxpo
