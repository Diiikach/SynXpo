#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "synxpo/client/grpc_client.h"

int main(int argc, char** argv) {
    std::string server_address("localhost:50051");
    
    if (argc > 1) {
        server_address = argv[1];
    }

    std::cout << "=== SynXpo Client Demo ===" << std::endl;
    std::cout << "Connecting to " << server_address << "..." << std::endl;

    // Create client
    synxpo::GRPCClient client(server_address);

    // Connect to server
    auto status = client.Connect();
    if (!status.ok()) {
        std::cerr << "Failed to connect: " << status.message() << std::endl;
        return 1;
    }
    std::cout << "✓ Connected successfully" << std::endl;

    // Set callback for unexpected messages
    client.SetMessageCallback([](const synxpo::ServerMessage& msg) {
        std::cout << "[Callback] Received message: ";
        if (msg.has_check_version()) {
            std::cout << "CheckVersion with " 
                      << msg.check_version().files_size() << " files" << std::endl;
        } else if (msg.has_file_write()) {
            std::cout << "FileWrite (" << msg.file_write().chunk().data().size() << " bytes)" << std::endl;
        } else if (msg.has_ok_directory_created()) {
            std::cout << "OkDirectoryCreated: " << msg.ok_directory_created().directory_id() << std::endl;
        } else if (msg.has_error()) {
            std::cout << "Error: " << msg.error().message() << std::endl;
        } else {
            std::cout << "Unknown message type" << std::endl;
        }
    });

    // Start receiving messages
    client.StartReceiving();
    std::cout << "✓ Started receiving messages" << std::endl;

    // Demo 1: Create directory
    std::cout << "\n--- Demo 1: Create directory ---" << std::endl;
    synxpo::ClientMessage request;
    request.mutable_directory_create();
    
    status = client.SendMessage(request);
    if (!status.ok()) {
        std::cerr << "Failed to send: " << status.message() << std::endl;
    } else {
        std::cout << "✓ Sent DirectoryCreate" << std::endl;
    }

    // Wait for response
    auto response = client.WaitForMessage(
        [](const synxpo::ServerMessage& msg) {
            return msg.has_ok_directory_created();
        },
        std::chrono::seconds(5));

    if (response.ok()) {
        std::cout << "✓ Received OkDirectoryCreated: " 
                  << response->ok_directory_created().directory_id() << std::endl;
    } else {
        std::cout << "✗ Failed to receive response: " << response.status().message() << std::endl;
    }

    // Demo 3: Timeout on wait
    std::cout << "\n--- Demo 3: Timeout demo ---" << std::endl;
    auto timeout_result = client.WaitForMessage(
        [](const synxpo::ServerMessage& msg) {
            return msg.has_version_increased();  // Wait for message that won't come
        },
        std::chrono::milliseconds(500));

    if (!timeout_result.ok()) {
        std::cout << "✓ Timeout as expected: " << timeout_result.status().message() << std::endl;
    }

    // Cleanup
    std::cout << "\n--- Shutting down ---" << std::endl;
    client.Disconnect();
    std::cout << "✓ Disconnected" << std::endl;

    return 0;
}
