#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"

#include "synxpo/client/config.h"
#include "synxpo/client/grpc_client.h"
#include "synxpo/client/synchronizer.h"
#include "synxpo/client/file_watcher.h"
#include "synxpo/client/logger.h"
#include "synxpo/common/in_memory_file_storage.h"

namespace {
std::atomic<bool> running{true};

void SignalHandler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down..." << std::endl;
    running.store(false);
}
}  // namespace

ABSL_FLAG(std::string, config_file, "synxpo_config.json", "Путь к файлу конфигурации");
ABSL_FLAG(bool, backup, false, "Совершить бэкап (заглушка)");
ABSL_FLAG(bool, restore, false, "Откатить из бэкапа (заглушка)");
ABSL_FLAG(std::string, add_watch_directory, "", "Добавить директорию для отслеживания");

ABSL_FLAG(std::string, set_server_address, "", "Изменить server_address в конфиге и сохранить");
ABSL_FLAG(std::string, set_storage_path, "", "Изменить storage_path в конфиге и сохранить");
ABSL_FLAG(std::string, set_backup_path, "", "Изменить backup_path в конфиге и сохранить");
ABSL_FLAG(std::string, set_temp_path, "", "Изменить temp_path в конфиге и сохранить");
ABSL_FLAG(int64_t, set_watch_debounce_ms, -1, "Изменить watch_debounce_ms в конфиге и сохранить");
ABSL_FLAG(int64_t, set_chunk_size, -1, "Изменить chunk_size в конфиге и сохранить");

int main(int argc, char** argv) {
    absl::SetProgramUsageMessage(
        "SynXpo — синхронизация директорий и резервное копирование.\n"
        "Режимы:\n"
        "  1) По умолчанию: запускает синхронизацию (auto sync).\n"
        "  2) Управление конфигом: --set_server_address=... и/или другие --set_* (сохранит и выйдет).\n"
        "  3) Добавить директорию: --add_watch_directory=/path (сохранит и выйдет).\n"
        "  4) Бэкап/откат: --backup / --restore (заглушки).\n"
        "Примеры:\n"
        "  ./synxpo-client --config_file=cfg.json --set_server_address=localhost:50051\n"
        "  ./synxpo-client --config_file=cfg.json --add_watch_directory=/home/user/Documents"
    );
    absl::ParseCommandLine(argc, argv);

    // Initialize logger early
    synxpo::Logger::Instance().SetLevel(synxpo::LogLevel::DEBUG);
    LOG_INFO("=== SynXpo Client starting ===");

    std::string config_path = absl::GetFlag(FLAGS_config_file);
    LOG_DEBUG("Config file path: " + config_path);
    bool do_backup = absl::GetFlag(FLAGS_backup);
    bool do_restore = absl::GetFlag(FLAGS_restore);
    std::string watch_dir = absl::GetFlag(FLAGS_add_watch_directory);

    std::string set_server_address = absl::GetFlag(FLAGS_set_server_address);
    std::string set_storage_path = absl::GetFlag(FLAGS_set_storage_path);
    std::string set_backup_path = absl::GetFlag(FLAGS_set_backup_path);
    std::string set_temp_path = absl::GetFlag(FLAGS_set_temp_path);
    int64_t set_watch_debounce_ms = absl::GetFlag(FLAGS_set_watch_debounce_ms);
    int64_t set_chunk_size = absl::GetFlag(FLAGS_set_chunk_size);

    bool has_config_update = !set_server_address.empty() || !set_storage_path.empty() ||
        !set_backup_path.empty() || !set_temp_path.empty() || set_watch_debounce_ms >= 0 ||
        set_chunk_size >= 0;

    int command_count = 0;
    if (has_config_update) {
        ++command_count;
    }
    if (!watch_dir.empty()) {
        ++command_count;
    }
    if (do_backup) {
        ++command_count;
    }
    if (do_restore) {
        ++command_count;
    }
    if (command_count > 1) {
        std::cerr << "Error: выбери только одну команду: config update / add_watch_directory / backup / restore" << std::endl;
        LOG_ERROR("Multiple conflicting commands specified");
        return 2;
    }

    // Управление конфигом и командами-параметрами
    synxpo::ClientConfig config;
    LOG_INFO("Loading configuration from: " + config_path);
    auto load_status = config.Load(config_path);
    if (!load_status.ok()) {
        LOG_WARNING("Config not found, using default values: " + std::string(load_status.message()));
        config.SetServerAddress("localhost:50051");
        config.SetStoragePath("./synxpo_storage");
        config.SetBackupPath("./synxpo_backup");
        config.SetTempPath("./synxpo_temp");
        config.SetWatchDebounce(std::chrono::milliseconds(500));
        config.SetChunkSize(1024 * 1024);  // 1 MB
    } else {
        LOG_INFO("Configuration loaded successfully");
    }

    if (has_config_update) {
        LOG_INFO("Updating configuration settings");
        if (!set_server_address.empty()) {
            config.SetServerAddress(set_server_address);
        }
        if (!set_storage_path.empty()) {
            config.SetStoragePath(set_storage_path);
        }
        if (!set_backup_path.empty()) {
            config.SetBackupPath(set_backup_path);
        }
        if (!set_temp_path.empty()) {
            config.SetTempPath(set_temp_path);
        }
        if (set_watch_debounce_ms >= 0) {
            config.SetWatchDebounce(std::chrono::milliseconds(set_watch_debounce_ms));
        }
        if (set_chunk_size >= 0) {
            config.SetChunkSize(static_cast<size_t>(set_chunk_size));
        }

        auto save_status = config.Save(config_path);
        if (!save_status.ok()) {
            std::cerr << "Failed to save config: " << save_status.message() << std::endl;
            LOG_ERROR("Failed to save config: " + std::string(save_status.message()));
            return 1;
        }
        std::cout << "Config updated and saved: " << config_path << std::endl;
        LOG_INFO("Configuration updated and saved to: " + config_path);
        return 0;
    }

    if (!watch_dir.empty()) {
        LOG_INFO("Adding watch directory: " + watch_dir);
        synxpo::DirectoryConfig dir;
        dir.directory_id.clear();
        dir.local_path = watch_dir;
        config.AddDirectory(dir);
        auto save_status = config.Save(config_path);
        if (!save_status.ok()) {
            std::cerr << "Failed to save config: " << save_status.message() << std::endl;
            LOG_ERROR("Failed to save config after adding directory: " + std::string(save_status.message()));
            return 1;
        }
        std::cout << "Добавлена директория для отслеживания: " << watch_dir << std::endl;
        LOG_INFO("Watch directory added successfully: " + watch_dir);
        return 0;
    }

    if (do_backup) {
        // TODO: сделать реальный backup, пока просто заглушка
        LOG_INFO("Backup mode requested (stub)");
        std::cout << "Выполняется бэкап (заглушка) ..." << std::endl;
        return 0;
    }

    if (do_restore) {
        // TODO: сделать реальный restore/rollback, пока просто заглушка
        LOG_INFO("Restore mode requested (stub)");
        std::cout << "Выполняется откат из бэкапа (заглушка) ..." << std::endl;
        return 0;
    }

    // Setup signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    LOG_INFO("Starting auto sync mode");
    LOG_INFO("Server address: " + config.GetServerAddress());
    LOG_INFO("Number of directories to sync: " + std::to_string(config.GetDirectories().size()));

    std::cout << "=== SynXpo Client ===" << std::endl;
    std::cout << "Server: " << config.GetServerAddress() << std::endl;
    std::cout << "Press Ctrl+C to stop" << std::endl;
    std::cout << std::endl;

    // Initialize components
    LOG_INFO("Initializing components...");
    synxpo::InMemoryFileMetadataStorage storage;
    synxpo::GRPCClient grpc_client(config.GetServerAddress());
    synxpo::FileWatcher file_watcher;
    synxpo::Synchronizer synchronizer(config, storage, grpc_client, file_watcher);
    
    // Set config path for saving updates
    synchronizer.SetConfigPath(config_path);

    // Connect to server
    LOG_INFO("Connecting to server: " + config.GetServerAddress());
    auto status = grpc_client.Connect();
    if (!status.ok()) {
        std::cerr << "Failed to connect: " << status.message() << std::endl;
        LOG_ERROR("Connection failed: " + std::string(status.message()));
        return 1;
    }
    LOG_INFO("Successfully connected to server");

    // Start receiving messages
    grpc_client.StartReceiving();
    LOG_INFO("Started receiving messages from server");

    // Start auto sync
    LOG_INFO("Starting auto sync...");
    status = synchronizer.StartAutoSync();
    if (!status.ok()) {
        std::cerr << "Failed to start auto sync: " << status.message() << std::endl;
        LOG_ERROR("Failed to start auto sync: " + std::string(status.message()));
        grpc_client.Disconnect();
        return 1;
    }
    
    std::cout << "Synchronization running. Monitoring for changes..." << std::endl;
    LOG_INFO("Auto sync started successfully. Monitoring for changes...");

    // Main loop - just wait for signal
    LOG_DEBUG("Entering main loop");
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Cleanup
    std::cout << "\nShutting down..." << std::endl;
    LOG_INFO("Shutting down client...");
    synchronizer.StopAutoSync();
    LOG_INFO("Auto sync stopped");
    grpc_client.Disconnect();
    LOG_INFO("Disconnected from server");
    LOG_INFO("=== SynXpo Client stopped ===");

    return 0;
}
