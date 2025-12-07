#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

class SyncServiceImpl final : public synxpo::SyncService::Service {
public:
    Status Stream(ServerContext* context,
                  ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) override {
        std::cout << "Client connected" << std::endl;
        
        // Send initial CheckVersion message on server's initiative
        std::cout << "Sending initial CheckVersion..." << std::endl;
        synxpo::ServerMessage initial_msg;
        auto* check = initial_msg.mutable_check_version();
        
        auto* file1 = check->add_files();
        file1->set_id("init-file-1");
        file1->set_directory_id("init-dir");
        file1->set_version(10);
        file1->set_content_changed_version(5);
        file1->set_type(synxpo::FILE);
        file1->set_current_path("initial.txt");
        file1->set_deleted(false);
        
        if (!stream->Write(initial_msg)) {
            std::cerr << "Failed to send initial message" << std::endl;
            return Status::OK;
        }
        std::cout << "Sent initial CheckVersion with 1 file" << std::endl;
        
        synxpo::ClientMessage client_msg;
        
        while (stream->Read(&client_msg)) {
            std::cout << "Received message: ";
            
            // Handle different message types
            if (client_msg.has_directory_create()) {
                std::cout << "DirectoryCreate" << std::endl;
                
                // Send response
                synxpo::ServerMessage response;
                response.mutable_ok_directory_created()->set_directory_id("test-directory-123");
                
                if (!stream->Write(response)) {
                    std::cerr << "Failed to write response" << std::endl;
                    break;
                }
                std::cout << "Sent OkDirectoryCreated" << std::endl;
                
            } else if (client_msg.has_directory_subscribe()) {
                std::cout << "DirectorySubscribe: " << client_msg.directory_subscribe().directory_id() << std::endl;
                
                synxpo::ServerMessage response;
                response.mutable_ok_subscribed()->set_directory_id(
                    client_msg.directory_subscribe().directory_id());
                
                stream->Write(response);
                std::cout << "Sent OkSubscribed" << std::endl;
                
            } else if (client_msg.has_directory_unsubscribe()) {
                std::cout << "DirectoryUnsubscribe: " << client_msg.directory_unsubscribe().directory_id() << std::endl;
                
                synxpo::ServerMessage response;
                response.mutable_ok_unsubscribed()->set_directory_id(
                    client_msg.directory_unsubscribe().directory_id());
                
                stream->Write(response);
                std::cout << "Sent OkUnsubscribed" << std::endl;
                
            } else if (client_msg.has_request_version()) {
                std::cout << "RequestVersion" << std::endl;
                
                // Send some dummy file metadata
                synxpo::ServerMessage response;
                auto* check = response.mutable_check_version();
                
                auto* file1 = check->add_files();
                file1->set_id("file-1");
                file1->set_directory_id("dir-1");
                file1->set_version(1);
                file1->set_content_changed_version(1);
                file1->set_type(synxpo::FILE);
                file1->set_current_path("test.txt");
                file1->set_deleted(false);
                
                auto* file2 = check->add_files();
                file2->set_id("file-2");
                file2->set_directory_id("dir-1");
                file2->set_version(2);
                file2->set_content_changed_version(1);
                file2->set_type(synxpo::FOLDER);
                file2->set_current_path("subdir");
                file2->set_deleted(false);
                
                stream->Write(response);
                std::cout << "Sent CheckVersion with 2 files" << std::endl;
                
            } else {
                std::cout << "Unknown message type" << std::endl;
            }
        }
        
        std::cout << "Client disconnected" << std::endl;
        return Status::OK;
    }
};

void RunServer(const std::string& server_address) {
    SyncServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    std::string server_address("0.0.0.0:50051");
    
    if (argc > 1) {
        server_address = argv[1];
    }

    RunServer(server_address);

    return 0;
}
