#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "synxpo/server/service.h"
#include "synxpo/server/storage.h"
#include "synxpo/server/subscriptions.h"

void PrintUsage(const char* program) {
    std::cout << "Usage: " << program << " [address]\n"
              << "\n"
              << "Arguments:\n"
              << "  address    Server address (default: 0.0.0.0:50051)\n"
              << "\n"
              << "Examples:\n"
              << "  " << program << "\n"
              << "  " << program << " localhost:50051\n"
              << "  " << program << " 0.0.0.0:8080\n";
}

void RunServer(const std::string& server_address) {
    synxpo::server::Storage storage;
    synxpo::server::SubscriptionManager subscriptions;
    synxpo::server::SyncServiceImpl service(storage, subscriptions);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    
    // Configure message size limits (for large files)
    builder.SetMaxReceiveMessageSize(16 * 1024 * 1024);  // 16 MB
    builder.SetMaxSendMessageSize(16 * 1024 * 1024);     // 16 MB

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    
    if (!server) {
        std::cerr << "Failed to start server on " << server_address << std::endl;
        return;
    }
    
    std::cout << "==================================================" << std::endl;
    std::cout << "SynXpo Server v1.0" << std::endl;
    std::cout << "Listening on " << server_address << std::endl;
    std::cout << "==================================================" << std::endl;
    std::cout << "\nPress Ctrl+C to stop the server.\n" << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    std::string server_address = "0.0.0.0:50051";
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            return 0;
        } else if (arg[0] != '-') {
            server_address = arg;
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            PrintUsage(argv[0]);
            return 1;
        }
    }

    RunServer(server_address);
    return 0;
}
