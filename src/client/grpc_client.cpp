#include "synxpo/client/grpc_client.h"

#include <algorithm>
#include <absl/strings/str_cat.h>

namespace synxpo {

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
    
    if (!stream_->Write(message)) {
        return absl::UnavailableError("Failed to write message to stream");
    }

    return absl::OkStatus();
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
            ProcessMessage(message);
        } else {
            // Stream closed or error
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
