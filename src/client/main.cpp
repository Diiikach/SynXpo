#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "synxpo.grpc.pb.h"
#include "synxpo/client/file_watcher.h"

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
    
    // Demo FileWatcher functionality
    std::cout << "\n=== FileWatcher Demo ===" << std::endl;
    
    // Create FileWatcher instance
    synxpo::FileWatcher watcher;
    std::cout << "FileWatcher created" << std::endl;
    
    // Set callback for file events
    watcher.SetEventCallback([](const synxpo::FileEvent& event) {
        std::cout << "Event detected: ";
        
        switch (event.type) {
            case synxpo::FileEventType::Created:
                std::cout << "CREATED";
                break;
            case synxpo::FileEventType::Modified:
                std::cout << "MODIFIED";
                break;
            case synxpo::FileEventType::Deleted:
                std::cout << "DELETED";
                break;
            case synxpo::FileEventType::Renamed:
                std::cout << "RENAMED";
                if (event.old_path) {
                    std::cout << " from " << event.old_path->string();
                }
                break;
        }
        
        std::cout << " - ";
        
        switch (event.entry_type) {
            case synxpo::FSEntryType::File:
                std::cout << "[FILE] ";
                break;
            case synxpo::FSEntryType::Directory:
                std::cout << "[DIR] ";
                break;
            case synxpo::FSEntryType::Unknown:
                std::cout << "[???] ";
                break;
        }
        
        std::cout << event.path.string() << std::endl;
    });
    std::cout << "Event callback set" << std::endl;
    
    // Add initial watch directory (before starting)
    std::string watch_path = "/tmp/synxpo_test";
    try {
        // Create test directory if it doesn't exist
        std::filesystem::create_directories(watch_path);
        
        watcher.AddWatch(watch_path, true);
        std::cout << "Added watch for: " << watch_path << " (recursive)" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to add watch: " << e.what() << std::endl;
        return 1;
    }
    
    // Add second watch directory (also before starting)
    std::string watch_path2 = "/tmp/synxpo_test2";
    try {
        std::filesystem::create_directories(watch_path2);
        watcher.AddWatch(watch_path2, false);  // non-recursive
        std::cout << "Added second watch: " << watch_path2 
                  << " (non-recursive)" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to add second watch: " << e.what() << std::endl;
    }
    
    // Start watching
    watcher.Start();
    std::cout << "FileWatcher started (running: " << std::boolalpha 
              << watcher.IsRunning() << ")" << std::endl;
    
    std::cout << "\nWatching for file changes. Try creating/modifying files in:" 
              << std::endl;
    std::cout << "  - " << watch_path << std::endl;
    std::cout << "  - " << watch_path2 << std::endl;
    std::cout << "Press Ctrl+C to stop..." << std::endl;
    
    // Keep running until interrupted
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
