#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"

namespace synxpo::server {

/// Manages client subscriptions to directories
class SubscriptionManager {
public:
    using StreamPtr = grpc::ServerReaderWriter<ServerMessage, ClientMessage>*;
    
    SubscriptionManager();
    ~SubscriptionManager();
    
    /// Subscribe a client to a directory
    void Subscribe(const std::string& client_id, 
                   const std::string& dir_id, 
                   StreamPtr stream);
    
    /// Unsubscribe a client from a directory
    void Unsubscribe(const std::string& client_id, const std::string& dir_id);
    
    /// Remove a client entirely (on disconnect)
    void RemoveClient(const std::string& client_id);
    
    /// Check if a client is subscribed to a directory
    bool IsSubscribed(const std::string& client_id, const std::string& dir_id) const;
    
    /// Get all directory IDs a client is subscribed to
    std::set<std::string> GetClientDirectories(const std::string& client_id) const;
    
    /// Notify all subscribers of a directory about changes
    /// @param except_client Client to exclude from notification (usually the sender)
    void NotifySubscribers(const std::string& dir_id,
                          const std::string& except_client,
                          const ServerMessage& message);
    
    /// Send a message to a specific client
    bool SendToClient(const std::string& client_id, const ServerMessage& message);

private:
    mutable std::shared_mutex mutex_;
    std::map<std::string, std::set<std::string>> subscriptions_;  // dir_id -> client_ids
    std::map<std::string, StreamPtr> client_streams_;              // client_id -> stream
    std::map<std::string, std::set<std::string>> client_dirs_;     // client_id -> dir_ids
};

}  // namespace synxpo::server
