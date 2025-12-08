#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

namespace {

std::string GenerateUuid() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t high = dis(gen);
    uint64_t low = dis(gen);

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

struct FileRecord {
    synxpo::FileMetadata meta;
    std::filesystem::path path;
};

struct DirectoryRecord {
    std::string id;
    std::filesystem::path root;
    std::map<std::string, FileRecord> files;  // file_id -> record
};

struct PendingUpload {
    std::string directory_id;
    std::string file_id;
    std::filesystem::path path;
};

class SyncServiceImpl final : public synxpo::SyncService::Service {
public:
    explicit SyncServiceImpl(std::filesystem::path storage_root)
        : storage_root_(std::move(storage_root)) {
        std::filesystem::create_directories(storage_root_);
    }

    Status Stream(ServerContext* context,
                  ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) override {
        std::cout << "Client connected" << std::endl;

        synxpo::ClientMessage client_msg;
        while (stream->Read(&client_msg)) {
            switch (client_msg.message_case()) {
                case synxpo::ClientMessage::kDirectoryCreate:
                    HandleDirectoryCreate(client_msg, stream);
                    break;
                case synxpo::ClientMessage::kDirectorySubscribe:
                    HandleDirectorySubscribe(client_msg, stream);
                    break;
                case synxpo::ClientMessage::kDirectoryUnsubscribe:
                    HandleDirectoryUnsubscribe(client_msg, stream);
                    break;
                case synxpo::ClientMessage::kRequestVersion:
                    HandleRequestVersion(client_msg, stream);
                    break;
                case synxpo::ClientMessage::kAskVersionIncrease:
                    HandleAskVersionIncrease(client_msg, stream);
                    break;
                case synxpo::ClientMessage::kRequestFileContent:
                    HandleRequestFileContent(client_msg, stream);
                    break;
                case synxpo::ClientMessage::kFileWrite:
                    HandleFileWrite(client_msg.file_write());
                    break;
                case synxpo::ClientMessage::kFileWriteEnd:
                    HandleFileWriteEnd(stream);
                    break;
                default:
                    break;
            }
        }

        CloseAllPendingWrites();
        std::cout << "Client disconnected" << std::endl;
        return Status::OK;
    }

private:
    void HandleDirectoryCreate(const synxpo::ClientMessage& msg,
                               ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        std::string dir_id = GenerateUuid();
        DirectoryRecord record;
        record.id = dir_id;
        record.root = storage_root_ / dir_id;
        std::filesystem::create_directories(record.root);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            directories_[dir_id] = std::move(record);
        }

        synxpo::ServerMessage response;
        if (msg.has_request_id()) {
            response.set_request_id(msg.request_id());
        }
        response.mutable_ok_directory_created()->set_directory_id(dir_id);
        stream->Write(response);
    }

    void HandleDirectorySubscribe(const synxpo::ClientMessage& msg,
                                  ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        synxpo::ServerMessage response;
        if (msg.has_request_id()) {
            response.set_request_id(msg.request_id());
        }

        const auto& dir_id = msg.directory_subscribe().directory_id();
        if (!DirectoryExists(dir_id)) {
            auto* error = response.mutable_error();
            error->set_code(synxpo::Error::DIRECTORY_NOT_FOUND);
            error->set_message("Directory not found");
        } else {
            response.mutable_ok_subscribed()->set_directory_id(dir_id);
        }

        stream->Write(response);
    }

    void HandleDirectoryUnsubscribe(const synxpo::ClientMessage& msg,
                                    ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        synxpo::ServerMessage response;
        if (msg.has_request_id()) {
            response.set_request_id(msg.request_id());
        }

        const auto& dir_id = msg.directory_unsubscribe().directory_id();
        if (!DirectoryExists(dir_id)) {
            auto* error = response.mutable_error();
            error->set_code(synxpo::Error::DIRECTORY_NOT_FOUND);
            error->set_message("Directory not found");
        } else {
            response.mutable_ok_unsubscribed()->set_directory_id(dir_id);
        }

        stream->Write(response);
    }

    void HandleRequestVersion(const synxpo::ClientMessage& msg,
                              ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        synxpo::ServerMessage response;
        if (msg.has_request_id()) {
            response.set_request_id(msg.request_id());
        }

        auto* check = response.mutable_check_version();

        for (const auto& req : msg.request_version().requests()) {
            if (req.has_directory_id()) {
                auto dir_id = req.directory_id();
                std::lock_guard<std::mutex> lock(mutex_);
                auto dir_it = directories_.find(dir_id);
                if (dir_it == directories_.end()) {
                    continue;
                }
                for (const auto& [_, rec] : dir_it->second.files) {
                    *check->add_files() = rec.meta;
                }
            } else if (req.has_file_id()) {
                const auto& file_id = req.file_id().id();
                const auto& dir_id = req.file_id().directory_id();

                std::lock_guard<std::mutex> lock(mutex_);
                auto dir_it = directories_.find(dir_id);
                if (dir_it == directories_.end()) {
                    continue;
                }
                auto file_it = dir_it->second.files.find(file_id);
                if (file_it != dir_it->second.files.end()) {
                    *check->add_files() = file_it->second.meta;
                }
            }
        }

        stream->Write(response);
    }

    void HandleAskVersionIncrease(const synxpo::ClientMessage& msg,
                                  ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        std::vector<PendingUpload> uploads;
        std::vector<synxpo::FileMetadata> updated_without_upload;

        for (const auto& file_info : msg.ask_version_increase().files()) {
            std::string dir_id = file_info.directory_id();
            std::lock_guard<std::mutex> lock(mutex_);
            auto dir_it = directories_.find(dir_id);
            if (dir_it == directories_.end()) {
                continue;
            }
            DirectoryRecord& dir = dir_it->second;

            std::string file_id = file_info.has_id() ? file_info.id() : GenerateUuid();
            auto& rec = dir.files[file_id];
            if (rec.meta.id().empty()) {
                rec.meta.set_id(file_id);
                rec.meta.set_directory_id(dir_id);
                rec.meta.set_version(0);
                rec.meta.set_content_changed_version(0);
            }

            rec.meta.set_current_path(file_info.current_path());
            rec.meta.set_type(file_info.type());
            rec.meta.set_deleted(file_info.deleted());

            rec.meta.set_version(rec.meta.version() + 1);
            if (file_info.content_changed()) {
                rec.meta.set_content_changed_version(rec.meta.content_changed_version() + 1);
            }

            rec.path = dir.root / rec.meta.current_path();

            if (rec.meta.deleted()) {
                std::error_code ec;
                std::filesystem::remove(rec.path, ec);
                updated_without_upload.push_back(rec.meta);
            } else if (file_info.content_changed()) {
                uploads.push_back({dir_id, file_id, rec.path});
            } else {
                updated_without_upload.push_back(rec.meta);
            }
        }

        synxpo::ServerMessage response;
        if (msg.has_request_id()) {
            response.set_request_id(msg.request_id());
        }

        if (!uploads.empty()) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (const auto& upload : uploads) {
                    pending_uploads_[UploadKey(upload.directory_id, upload.file_id)] = upload;
                }
            }
            response.mutable_version_increase_allow();
        } else {
            auto* increased = response.mutable_version_increased();
            for (const auto& meta : updated_without_upload) {
                *increased->add_files() = meta;
            }
        }

        stream->Write(response);
    }

    void HandleRequestFileContent(const synxpo::ClientMessage& msg,
                                  ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        synxpo::ServerMessage allow;
        if (msg.has_request_id()) {
            allow.set_request_id(msg.request_id());
        }
        allow.mutable_file_content_request_allow();
        stream->Write(allow);

        constexpr size_t kChunkSize = 1 * 1024 * 1024;
        std::vector<char> buffer(kChunkSize);

        for (const auto& file_req : msg.request_file_content().files()) {
            const std::string& dir_id = file_req.directory_id();
            const std::string& file_id = file_req.id();

            std::filesystem::path file_path;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto dir_it = directories_.find(dir_id);
                if (dir_it == directories_.end()) {
                    continue;
                }
                auto file_it = dir_it->second.files.find(file_id);
                if (file_it == dir_it->second.files.end() || file_it->second.meta.deleted()) {
                    continue;
                }
                file_path = file_it->second.path;
            }

            std::ifstream in(file_path, std::ios::binary);
            if (!in) {
                continue;
            }

            while (in.read(buffer.data(), buffer.size()) || in.gcount() > 0) {
                synxpo::ServerMessage chunk_msg;
                auto* file_write = chunk_msg.mutable_file_write();
                auto* chunk = file_write->mutable_chunk();
                chunk->set_id(file_id);
                chunk->set_directory_id(dir_id);
                chunk->set_data(buffer.data(), static_cast<size_t>(in.gcount()));
                stream->Write(chunk_msg);
            }
        }

        synxpo::ServerMessage end_msg;
        end_msg.mutable_file_write_end();
        stream->Write(end_msg);
    }

    void HandleFileWrite(const synxpo::FileWrite& msg) {
        const auto& chunk = msg.chunk();
        std::string key = UploadKey(chunk.directory_id(), chunk.id());

        auto upload_opt = GetPendingUpload(key);
        if (!upload_opt) {
            return;
        }

        auto& upload = *upload_opt;
        {
            std::lock_guard<std::mutex> lock(write_mutex_);
            auto& stream = open_writes_[key];
            if (!stream.is_open()) {
                std::filesystem::create_directories(upload.path.parent_path());
                stream.open(upload.path, std::ios::binary | std::ios::trunc);
            }
            stream.write(chunk.data().data(), static_cast<std::streamsize>(chunk.data().size()));
        }
    }

    void HandleFileWriteEnd(ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) {
        CloseAllPendingWrites();

        // Prepare VersionIncreased for uploaded files
        synxpo::ServerMessage msg;
        auto* increased = msg.mutable_version_increased();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& [key, upload] : pending_uploads_) {
                auto dir_it = directories_.find(upload.directory_id);
                if (dir_it == directories_.end()) {
                    continue;
                }
                auto file_it = dir_it->second.files.find(upload.file_id);
                if (file_it == dir_it->second.files.end()) {
                    continue;
                }
                *increased->add_files() = file_it->second.meta;
            }
            pending_uploads_.clear();
        }

        if (increased->files_size() > 0) {
            stream->Write(msg);
        }
    }

    bool DirectoryExists(const std::string& dir_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return directories_.find(dir_id) != directories_.end();
    }

    std::optional<PendingUpload> GetPendingUpload(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = pending_uploads_.find(key);
        if (it == pending_uploads_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void CloseAllPendingWrites() {
        std::lock_guard<std::mutex> lock(write_mutex_);
        for (auto& [_, stream] : open_writes_) {
            if (stream.is_open()) {
                stream.close();
            }
        }
        open_writes_.clear();
    }

    static std::string UploadKey(const std::string& dir_id, const std::string& file_id) {
        return dir_id + ":" + file_id;
    }

    std::filesystem::path storage_root_;
    std::mutex mutex_;
    std::unordered_map<std::string, DirectoryRecord> directories_;

    std::unordered_map<std::string, PendingUpload> pending_uploads_;  // key -> upload info
    std::mutex write_mutex_;
    std::unordered_map<std::string, std::ofstream> open_writes_;  // key -> stream
};

}  // namespace

void RunServer(const std::string& server_address, const std::filesystem::path& storage_root) {
    SyncServiceImpl service(storage_root);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address
              << " with storage at " << storage_root << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    std::string server_address("0.0.0.0:50051");
    std::filesystem::path storage_root = "server_storage";

    if (argc > 1) {
        server_address = argv[1];
    }
    if (argc > 2) {
        storage_root = argv[2];
    }

    RunServer(server_address, storage_root);

    return 0;
}
