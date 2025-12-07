#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class SyncServiceImpl final : public synxpo::SyncService::Service {
    // Service implementation will be added here
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
