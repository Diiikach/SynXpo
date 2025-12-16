#include "synxpo/server/service.h"
#include "synxpo/server/uuid.h"

#include <absl/log/log.h>

namespace synxpo::server {

SyncServiceImpl::SyncServiceImpl(Storage& storage,
                                   SubscriptionManager& subscriptions,
                                   const ServiceConfig& config)
    : storage_(storage)
    , subscriptions_(subscriptions)
    , config_(config) {}

SyncServiceImpl::~SyncServiceImpl() = default;

grpc::Status SyncServiceImpl::Stream(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) {
    
    std::string client_id = GenerateUuid();
    LOG(INFO) << "\n[Server] Client connected: " << client_id;
    
    ClientMessage client_msg;
    std::optional<PendingUpload> pending_upload;
    
    while (stream->Read(&client_msg)) {
        // Check for upload timeout if we're waiting for content
        if (pending_upload.has_value()) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - pending_upload->last_write_time);
            
            std::chrono::seconds timeout = pending_upload->received_first_write 
                ? config_.write_timeout 
                : config_.first_write_timeout;
            
            if (elapsed > timeout) {
                LOG(INFO) << "[Server] Upload timeout for client " << client_id;
                storage_.RollbackUpload(client_id, pending_upload->request);
                pending_upload.reset();
                
                ServerMessage error_msg;
                auto* error = error_msg.mutable_error();
                error->set_code(Error::TIMEOUT);
                error->set_message("Upload timeout");
                stream->Write(error_msg);
                continue;
            }
        }
        
        // Route to appropriate handler
        if (client_msg.has_directory_create()) {
            HandleDirectoryCreate(client_id, client_msg, stream);
        } else if (client_msg.has_directory_subscribe()) {
            HandleDirectorySubscribe(client_id, client_msg, stream);
        } else if (client_msg.has_directory_unsubscribe()) {
            HandleDirectoryUnsubscribe(client_id, client_msg, stream);
        } else if (client_msg.has_request_version()) {
            HandleRequestVersion(client_id, client_msg, stream);
        } else if (client_msg.has_ask_version_increase()) {
            HandleAskVersionIncrease(client_id, client_msg, stream, pending_upload);
        } else if (client_msg.has_file_write()) {
            HandleFileWrite(client_id, client_msg, pending_upload);
        } else if (client_msg.has_file_write_end()) {
            HandleFileWriteEnd(client_id, client_msg, stream, pending_upload);
        } else if (client_msg.has_request_file_content()) {
            HandleRequestFileContent(client_id, client_msg, stream);
        } else {
            LOG(INFO) << "[Server] Unknown message type from " << client_id;
        }
    }
    
    // Client disconnected
    LOG(INFO) << "\n[Server] Client disconnected: " << client_id;
    
    // Clean up any pending upload
    if (pending_upload.has_value()) {
        storage_.RollbackUpload(client_id, pending_upload->request);
    }
    
    subscriptions_.RemoveClient(client_id);
    storage_.ReleaseLocks(client_id);
    
    return grpc::Status::OK;
}

void SyncServiceImpl::HandleDirectoryCreate(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) {
    
    LOG(INFO) << "[Server] DirectoryCreate from " << client_id;
    
    std::string dir_id = storage_.CreateDirectory();
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    response.mutable_ok_directory_created()->set_directory_id(dir_id);
    stream->Write(response);
}

void SyncServiceImpl::HandleDirectorySubscribe(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) {
    
    const auto& subscribe = msg.directory_subscribe();
    LOG(INFO) << "[Server] DirectorySubscribe: " << subscribe.directory_id() 
              << " from " << client_id;
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    
    if (!storage_.DirectoryExists(subscribe.directory_id())) {
        auto* error = response.mutable_error();
        error->set_code(Error::DIRECTORY_NOT_FOUND);
        error->set_message("Directory not found: " + subscribe.directory_id());
        stream->Write(response);
        return;
    }
    
    subscriptions_.Subscribe(client_id, subscribe.directory_id(), stream);
    response.mutable_ok_subscribed()->set_directory_id(subscribe.directory_id());
    stream->Write(response);
}

void SyncServiceImpl::HandleDirectoryUnsubscribe(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) {
    
    const auto& unsubscribe = msg.directory_unsubscribe();
    LOG(INFO) << "[Server] DirectoryUnsubscribe: " << unsubscribe.directory_id() 
              << " from " << client_id;
    
    subscriptions_.Unsubscribe(client_id, unsubscribe.directory_id());
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    response.mutable_ok_unsubscribed()->set_directory_id(unsubscribe.directory_id());
    stream->Write(response);
}

void SyncServiceImpl::HandleRequestVersion(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) {
    
    const auto& req = msg.request_version();
    LOG(INFO) << "[Server] RequestVersion from " << client_id 
              << " with " << req.requests_size() << " requests";
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    
    auto* check = response.mutable_check_version();
    
    for (const auto& file_req : req.requests()) {
        if (file_req.has_directory_id()) {
            // Get all files in directory
            auto files = storage_.GetDirectoryFiles(file_req.directory_id());
            for (const auto& file : files) {
                *check->add_files() = file;
            }
            LOG(INFO) << "[Server] Added " << files.size() 
                      << " files from directory " << file_req.directory_id();
        } else if (file_req.has_file_id()) {
            // Get specific file
            auto file = storage_.GetFile(
                file_req.file_id().directory_id(),
                file_req.file_id().id());
            if (file.has_value()) {
                auto* meta = check->add_files();
                meta->set_id(file->id);
                meta->set_directory_id(file->directory_id);
                meta->set_version(file->version);
                meta->set_content_changed_version(file->content_changed_version);
                meta->set_type(file->type);
                meta->set_current_path(file->current_path);
                meta->set_deleted(file->deleted);
            }
        }
    }
    
    LOG(INFO) << "[Server] Sending CheckVersion with " << check->files_size() 
              << " files";
    stream->Write(response);
}

void SyncServiceImpl::HandleAskVersionIncrease(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream,
    std::optional<PendingUpload>& pending_upload) {
    
    const auto& ask = msg.ask_version_increase();
    LOG(INFO) << "[Server] AskVersionIncrease from " << client_id 
              << " with " << ask.files_size() << " files";
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    
    // Check if version increase is allowed
    auto results = storage_.CheckVersionIncrease(client_id, ask);
    
    // Check if any files are blocked or denied
    bool all_free = true;
    std::vector<FileStatusInfo> status_list;
    
    for (const auto& result : results) {
        FileStatusInfo status_info;
        status_info.set_id(result.file_id);
        status_info.set_directory_id(result.directory_id);
        status_info.set_status(result.status);
        status_list.push_back(status_info);
        
        if (result.status != FREE) {
            all_free = false;
        }
    }
    
    if (!all_free) {
        auto* deny = response.mutable_version_increase_deny();
        for (const auto& status_info : status_list) {
            *deny->add_files() = status_info;
        }
        stream->Write(response);
        LOG(INFO) << "[Server] Sent VersionIncreaseDeny";
        return;
    }
    
    // Check if any files need content upload
    bool needs_content = false;
    for (const auto& file : ask.files()) {
        if (file.content_changed() && !file.deleted()) {
            needs_content = true;
            break;
        }
    }
    
    if (needs_content) {
        // Lock files and wait for content
        storage_.LockFilesForWrite(client_id, ask);
        
        pending_upload = PendingUpload{};
        pending_upload->request = ask;
        pending_upload->last_write_time = std::chrono::steady_clock::now();
        pending_upload->received_first_write = false;
        
        response.mutable_version_increase_allow();
        stream->Write(response);
        LOG(INFO) << "[Server] Sent VersionIncreaseAllow, waiting for content";
    } else {
        // No content needed - apply changes immediately
        // Lock, apply, and release in one operation
        storage_.LockFilesForWrite(client_id, ask);
        auto updated = storage_.ApplyVersionIncrease(client_id, ask, {});
        
        auto* increased = response.mutable_version_increased();
        for (const auto& file : updated) {
            *increased->add_files() = file;
        }
        stream->Write(response);
        LOG(INFO) << "[Server] Sent VersionIncreased (no content change)";
        
        // Notify other subscribers
        if (!updated.empty()) {
            NotifyFileChanges(updated[0].directory_id(), client_id, updated);
        }
    }
}

void SyncServiceImpl::HandleFileWrite(
    const std::string& client_id,
    const ClientMessage& msg,
    std::optional<PendingUpload>& pending_upload) {
    
    const auto& write = msg.file_write();
    const auto& chunk = write.chunk();
    
    if (!pending_upload.has_value()) {
        LOG(ERROR) << "[Server] FileWrite without pending upload from " << client_id;
        return;
    }
    
    pending_upload->received_first_write = true;
    pending_upload->last_write_time = std::chrono::steady_clock::now();
    
    // Determine which file this chunk belongs to using current_path (most reliable)
    // or file ID as fallback
    std::string file_key;
    
    // Prefer current_path as it works for both new and existing files
    if (!chunk.current_path().empty()) {
        file_key = chunk.current_path();
    } else if (!chunk.id().empty()) {
        // Use file ID as key
        file_key = chunk.id();
    } else {
        // Fallback: find first file that needs content
        for (const auto& file : pending_upload->request.files()) {
            if (file.content_changed() && !file.deleted()) {
                file_key = file.current_path();
                break;
            }
        }
    }
    
    if (file_key.empty()) {
        LOG(ERROR) << "[Server] Could not determine file for chunk";
        return;
    }
    
    // Append chunk data
    auto& content = pending_upload->file_contents[file_key];
    const auto& data = chunk.data();
    
    // Handle offset (for potential out-of-order or retry scenarios)
    if (chunk.offset() >= content.size()) {
        content.resize(chunk.offset() + data.size());
    }
    std::copy(data.begin(), data.end(), content.begin() + chunk.offset());
    
    LOG(INFO) << "[Server] Received FileWrite chunk: offset=" << chunk.offset()
              << " size=" << data.size() 
              << " total=" << content.size()
              << " for " << file_key;
}

void SyncServiceImpl::HandleFileWriteEnd(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream,
    std::optional<PendingUpload>& pending_upload) {
    
    LOG(INFO) << "[Server] FileWriteEnd from " << client_id;
    
    if (!pending_upload.has_value()) {
        LOG(ERROR) << "[Server] FileWriteEnd without pending upload!";
        return;
    }
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    
    // Apply the version increase with content
    auto updated = storage_.ApplyVersionIncrease(
        client_id,
        pending_upload->request,
        pending_upload->file_contents);
    
    auto* increased = response.mutable_version_increased();
    for (const auto& file : updated) {
        *increased->add_files() = file;
    }
    stream->Write(response);
    
    LOG(INFO) << "[Server] Sent VersionIncreased with " << updated.size() << " files";
    
    // Notify other subscribers
    if (!updated.empty()) {
        NotifyFileChanges(updated[0].directory_id(), client_id, updated);
    }
    
    pending_upload.reset();
}

void SyncServiceImpl::HandleRequestFileContent(
    const std::string& client_id,
    const ClientMessage& msg,
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream) {
    
    const auto& req = msg.request_file_content();
    LOG(INFO) << "[Server] RequestFileContent from " << client_id 
              << " for " << req.files_size() << " files";
    
    ServerMessage response;
    if (msg.has_request_id()) {
        response.set_request_id(msg.request_id());
    }
    
    // Check if files can be read
    auto results = storage_.CheckFilesForRead(client_id, req);
    
    bool all_free = true;
    std::vector<FileStatusInfo> blocked_files;
    
    for (const auto& result : results) {
        if (result.status != FREE) {
            all_free = false;
            FileStatusInfo status_info;
            status_info.set_id(result.file_id);
            status_info.set_directory_id(result.directory_id);
            status_info.set_status(result.status);
            blocked_files.push_back(status_info);
        }
    }
    
    if (!all_free) {
        auto* deny = response.mutable_file_content_request_deny();
        for (const auto& status_info : blocked_files) {
            *deny->add_files() = status_info;
        }
        stream->Write(response);
        LOG(INFO) << "[Server] Sent FileContentRequestDeny";
        return;
    }
    
    // Lock files for reading
    storage_.LockFilesForRead(client_id, req);
    
    // Send allow
    response.mutable_file_content_request_allow();
    stream->Write(response);
    LOG(INFO) << "[Server] Sent FileContentRequestAllow";
    
    // Send file contents
    for (const auto& file_id : req.files()) {
        auto file = storage_.GetFile(file_id.directory_id(), file_id.id());
        if (!file.has_value()) {
            LOG(ERROR) << "[Server] File not found: " << file_id.id();
            continue;
        }
        
        const auto& content = file->content;
        const size_t chunk_size = config_.max_chunk_size;
        
        LOG(INFO) << "[Server] Sending file " << file->current_path 
                  << " size=" << content.size();
        
        // Send content in chunks
        for (size_t offset = 0; offset < content.size() || (offset == 0 && content.empty()); ) {
            ServerMessage write_msg;
            auto* write = write_msg.mutable_file_write();
            auto* chunk = write->mutable_chunk();
            
            chunk->set_id(file->id);
            chunk->set_directory_id(file->directory_id);
            chunk->set_offset(offset);
            chunk->set_current_path(file->current_path);  // Important for new clients
            
            size_t end = std::min(offset + chunk_size, content.size());
            if (end > offset) {
                chunk->set_data(content.data() + offset, end - offset);
            }
            
            stream->Write(write_msg);
            LOG(INFO) << "[Server] Sent chunk: offset=" << offset 
                      << " size=" << (end - offset);
            
            offset = end;
            if (content.empty()) break;
        }
    }
    
    // Send end marker
    ServerMessage end_msg;
    end_msg.mutable_file_write_end();
    stream->Write(end_msg);
    LOG(INFO) << "[Server] Sent FileWriteEnd";
    
    // Unlock files
    storage_.UnlockFilesAfterRead(client_id, req);
}

void SyncServiceImpl::NotifyFileChanges(const std::string& dir_id,
                                         const std::string& except_client,
                                         const std::vector<FileMetadata>& files) {
    // Send ALL files in the directory, not just changed ones
    // This ensures clients can properly diff their local state
    auto all_files = storage_.GetDirectoryFiles(dir_id);
    
    ServerMessage notification;
    auto* check = notification.mutable_check_version();
    for (const auto& file : all_files) {
        *check->add_files() = file;
    }
    
    LOG(INFO) << "[Server] Notifying subscribers of " << dir_id 
              << " about " << files.size() << " changed files (sending all " 
              << all_files.size() << " files)";
    subscriptions_.NotifySubscribers(dir_id, except_client, notification);
}

void SyncServiceImpl::SendError(
    grpc::ServerReaderWriter<ServerMessage, ClientMessage>* stream,
    const std::string& request_id,
    Error::ErrorCode code,
    const std::string& message) {
    
    ServerMessage response;
    if (!request_id.empty()) {
        response.set_request_id(request_id);
    }
    auto* error = response.mutable_error();
    error->set_code(code);
    error->set_message(message);
    stream->Write(response);
}

}  // namespace synxpo::server
