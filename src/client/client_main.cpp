#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class SyncClient {
public:
    SyncClient(std::shared_ptr<Channel> channel)
        : stub_(synxpo::SyncService::NewStub(channel)) {}

    // Client methods will be added here

private:
    std::unique_ptr<synxpo::SyncService::Stub> stub_;
};

int main(int argc, char** argv) {
    std::string server_address("localhost:50051");
    
    if (argc > 1) {
        server_address = argv[1];
    }

    auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    SyncClient client(channel);

    std::cout << "Client connected to " << server_address << std::endl;
    std::cout << "Client ready (implementation pending)" << std::endl;

    return 0;
}
