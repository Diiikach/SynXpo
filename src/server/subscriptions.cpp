#include "synxpo/server/subscriptions.h"

#include <iostream>

namespace synxpo::server {

SubscriptionManager::SubscriptionManager() = default;
SubscriptionManager::~SubscriptionManager() = default;

void SubscriptionManager::Subscribe(const std::string& client_id,
                                     const std::string& dir_id,
                                     StreamPtr stream) {
    std::unique_lock lock(mutex_);
    subscriptions_[dir_id].insert(client_id);
    client_streams_[client_id] = stream;
    client_dirs_[client_id].insert(dir_id);
    std::cout << "[Subscriptions] Client " << client_id 
              << " subscribed to " << dir_id << std::endl;
}

void SubscriptionManager::Unsubscribe(const std::string& client_id,
                                       const std::string& dir_id) {
    std::unique_lock lock(mutex_);
    subscriptions_[dir_id].erase(client_id);
    client_dirs_[client_id].erase(dir_id);
    std::cout << "[Subscriptions] Client " << client_id 
              << " unsubscribed from " << dir_id << std::endl;
}

void SubscriptionManager::RemoveClient(const std::string& client_id) {
    std::unique_lock lock(mutex_);
    
    // Remove from all subscriptions
    for (const auto& dir_id : client_dirs_[client_id]) {
        subscriptions_[dir_id].erase(client_id);
    }
    
    client_dirs_.erase(client_id);
    client_streams_.erase(client_id);
    
    std::cout << "[Subscriptions] Removed client " << client_id << std::endl;
}

bool SubscriptionManager::IsSubscribed(const std::string& client_id,
                                        const std::string& dir_id) const {
    std::shared_lock lock(mutex_);
    
    auto it = subscriptions_.find(dir_id);
    if (it == subscriptions_.end()) {
        return false;
    }
    return it->second.count(client_id) > 0;
}

std::set<std::string> SubscriptionManager::GetClientDirectories(
    const std::string& client_id) const {
    std::shared_lock lock(mutex_);
    
    auto it = client_dirs_.find(client_id);
    if (it == client_dirs_.end()) {
        return {};
    }
    return it->second;
}

void SubscriptionManager::NotifySubscribers(const std::string& dir_id,
                                             const std::string& except_client,
                                             const ServerMessage& message) {
    std::shared_lock lock(mutex_);
    
    auto sub_it = subscriptions_.find(dir_id);
    if (sub_it == subscriptions_.end()) {
        return;
    }
    
    for (const auto& client_id : sub_it->second) {
        if (client_id == except_client) {
            continue;
        }
        
        auto stream_it = client_streams_.find(client_id);
        if (stream_it != client_streams_.end() && stream_it->second) {
            if (stream_it->second->Write(message)) {
                std::cout << "[Subscriptions] Notified client " << client_id << std::endl;
            } else {
                std::cerr << "[Subscriptions] Failed to notify client " << client_id << std::endl;
            }
        }
    }
}

bool SubscriptionManager::SendToClient(const std::string& client_id,
                                        const ServerMessage& message) {
    std::shared_lock lock(mutex_);
    
    auto stream_it = client_streams_.find(client_id);
    if (stream_it == client_streams_.end() || !stream_it->second) {
        return false;
    }
    
    return stream_it->second->Write(message);
}

}  // namespace synxpo::server
