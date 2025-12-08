#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>

#include "synxpo/client/config.h"
#include "synxpo/client/grpc_client.h"
#include "synxpo/client/synchronizer.h"
#include "synxpo/client/file_watcher.h"
#include "synxpo/common/in_memory_file_storage.h"

namespace {
std::atomic<bool> running{true};

void SignalHandler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down..." << std::endl;
    running.store(false);
}
}  // namespace

int main(int argc, char** argv) {
    std::string server_address("localhost:50051");
    std::string config_path("synxpo_config.json");
    
    if (argc > 1) {
        server_address = argv[1];
    }
    if (argc > 2) {
        config_path = argv[2];
    }

    // Setup signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    std::cout << "=== SynXpo Client ===" << std::endl;
    std::cout << "Server: " << server_address << std::endl;
    std::cout << "Config: " << config_path << std::endl;
    std::cout << "Press Ctrl+C to stop" << std::endl;
    std::cout << std::endl;

    // Initialize components
    synxpo::ClientConfig config;
    
    // Try to load config, create default if it doesn't exist
    auto load_status = config.Load(config_path);
    if (!load_status.ok()) {
        std::cout << "Config not found, using defaults" << std::endl;
        config.SetServerAddress(server_address);
        config.SetStoragePath("./synxpo_storage");
        config.SetBackupPath("./synxpo_backup");
        config.SetTempPath("./synxpo_temp");
        config.SetWatchDebounce(std::chrono::milliseconds(500));
        config.SetChunkSize(1024 * 1024);  // 1 MB
    }

    synxpo::InMemoryFileMetadataStorage storage;
    synxpo::GRPCClient grpc_client(config.GetServerAddress());
    synxpo::FileWatcher file_watcher;
    synxpo::Synchronizer synchronizer(config, storage, grpc_client, file_watcher);

    // Connect to server
    std::cout << "Connecting to server..." << std::endl;
    auto status = grpc_client.Connect();
    if (!status.ok()) {
        std::cerr << "Failed to connect: " << status.message() << std::endl;
        return 1;
    }
    std::cout << "✓ Connected" << std::endl;

    // Start receiving messages
    grpc_client.StartReceiving();
    std::cout << "✓ Started receiving messages" << std::endl;

    // Start auto sync
    std::cout << "Starting auto sync..." << std::endl;
    status = synchronizer.StartAutoSync();
    if (!status.ok()) {
        std::cerr << "Failed to start auto sync: " << status.message() << std::endl;
        grpc_client.Disconnect();
        return 1;
    }
    std::cout << "✓ Auto sync started" << std::endl;
    std::cout << std::endl;
    std::cout << "Synchronization is running. Monitoring for changes..." << std::endl;

    // Main loop - just wait for signal
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Cleanup
    std::cout << "\nShutting down..." << std::endl;
    synchronizer.StopAutoSync();
    grpc_client.Disconnect();
    std::cout << "✓ Stopped" << std::endl;

    return 0;
}
