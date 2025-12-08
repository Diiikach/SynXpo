#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "synxpo.grpc.pb.h"

namespace synxpo {

class GRPCClient {
public:
    explicit GRPCClient(const std::string& server_address);
    ~GRPCClient();

    GRPCClient(const GRPCClient&) = delete;
    GRPCClient& operator=(const GRPCClient&) = delete;
    GRPCClient(GRPCClient&&) = delete;
    GRPCClient& operator=(GRPCClient&&) = delete;

    absl::Status Connect();
    void Disconnect();
    bool IsConnected() const;

    // Send message without waiting for response
    absl::Status SendMessage(const ClientMessage& message);
    
    // Send message and wait for response with matching request_id
    absl::StatusOr<ServerMessage> SendMessageWithResponse(
        ClientMessage& message,
        std::chrono::milliseconds timeout = std::chrono::seconds(30));
    
    using ServerMessageCallback = std::function<void(const ServerMessage& message)>;
    void SetMessageCallback(ServerMessageCallback callback);
    
    // Manage recieveing messages from server
    void StartReceiving();
    void StopReceiving();
    bool IsReceiving() const;

    using MessagePredicate = std::function<bool(const ServerMessage&)>;
    
    // Block until a message matching predicate is received
    absl::StatusOr<ServerMessage> WaitForMessage(
        MessagePredicate predicate,
        std::chrono::milliseconds timeout = std::chrono::seconds(30));

private:
    void ReceiveLoop();
    void ProcessMessage(const ServerMessage& message);
    void CallbackWorkerLoop();
    
    // Generate UUID v4 for request_id
    static std::string GenerateUuid();

    struct Waiter {
        MessagePredicate predicate;
        std::condition_variable cv;
        std::mutex mutex;
        std::optional<ServerMessage> result;
        bool done = false;
        bool cancelled = false;
    };

    std::string server_address_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<SyncService::Stub> stub_;
    
    std::unique_ptr<grpc::ClientContext> stream_context_;
    std::unique_ptr<grpc::ClientReaderWriter<ClientMessage, ServerMessage>> stream_;
    
    std::atomic<bool> connected_{false};
    
    std::thread receive_thread_;
    std::atomic<bool> receiving_{false};
    std::atomic<bool> should_stop_{false};
    
    std::mutex stream_mutex_;
    std::mutex waiters_mutex_;
    std::vector<std::shared_ptr<Waiter>> waiters_;
    
    ServerMessageCallback message_callback_;
    
    std::thread callback_worker_;
    std::queue<ServerMessage> callback_queue_;
    std::mutex callback_mutex_;
    std::condition_variable callback_cv_;
};

}  // namespace synxpo
