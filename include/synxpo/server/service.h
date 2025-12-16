#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"

#include "synxpo/server/storage.h"
#include "synxpo/server/subscriptions.h"

namespace synxpo::server {

/// Configuration for the sync service
struct ServiceConfig {
    std::chrono::seconds first_write_timeout{10};   // Timeout for first FILE_WRITE after ALLOW
    std::chrono::seconds write_timeout{30};         // Timeout between FILE_WRITE messages
    size_t max_chunk_size = 1024 * 1024;            // 1 MB max chunk size
};

/// State for a pending file upload
struct PendingUpload {
    AskVersionIncrease request;
    std::map<std::string, std::vector<uint8_t>> file_contents;
    std::chrono::steady_clock::time_point last_write_time;
    bool received_first_write = false;
};

/// gRPC service implementation for file synchronization
class SyncServiceImpl final : public SyncService::Service {
public:
    SyncServiceImpl(Storage& storage, 
                    SubscriptionManager& subscriptions,
                    const ServiceConfig& config = {});
    ~SyncServiceImpl();
    
    grpc::Status Stream(
        grpc::ServerContext* context,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) override;

private:
    // Message handlers
    void HandleDirectoryCreate(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream);
    
    void HandleDirectorySubscribe(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream);
    
    void HandleDirectoryUnsubscribe(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream);
    
    void HandleRequestVersion(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream);
    
    void HandleAskVersionIncrease(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream,
        std::optional<PendingUpload>& pending_upload);
    
    void HandleFileWrite(
        const std::string& client_id,
        const ClientMessage& msg,
        std::optional<PendingUpload>& pending_upload);
    
    void HandleFileWriteEnd(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream,
        std::optional<PendingUpload>& pending_upload);
    
    void HandleRequestFileContent(
        const std::string& client_id,
        const ClientMessage& msg,
        grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream);
    
    // Helper methods
    void NotifyFileChanges(const std::string& dir_id,
                          const std::string& except_client,
                          const std::vector<FileMetadata>& files);
    
    void SendError(grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream,
                   const std::string& request_id,
                   Error::ErrorCode code,
                   const std::string& message);
    
    Storage& storage_;
    SubscriptionManager& subscriptions_;
    ServiceConfig config_;
};

}  // namespace synxpo::server
