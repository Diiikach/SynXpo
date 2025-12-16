#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <cstdlib>
#include <unistd.h>
#include <filesystem>
#include <pwd.h>

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

std::string ExpandPath(const std::string& path) {
    if (path.empty() || path[0] != '~') {
        return path;
    }
    
    const char* home = std::getenv("HOME");
    if (home == nullptr) {
        home = getpwuid(getuid())->pw_dir;
    }
    
    if (path.size() == 1) {
        return std::string(home);
    }
    
    if (path[1] == '/') {
        return std::string(home) + path.substr(1);
    }
    
    return path;  // Return as-is if not in format ~/...
}
}  // namespace

ABSL_FLAG(std::string, config, "~/.config/synxpo/config.json", "Путь к файлу конфигурации");
ABSL_FLAG(std::string, path, "", "Путь для команды dir-pull");
ABSL_FLAG(std::string, name, "", "Имя директории для синхронизации");

int main(int argc, char** argv) {
    absl::SetProgramUsageMessage(
        "SynXpo — синхронизация директорий.\n\n"
        "Использование:\n"
        "  program [--config path] <command> [args...]\n\n"
        "Команды:\n"
        "  sync                    Запустить синхронизацию (по умолчанию)\n"
        "  dir-link <path>         Добавить директорию для отслеживания\n"
        "  dir-pull <id>           Подтянуть директорию с сервера\n"
        "  config set <key> <value> Изменить параметр конфигурации\n\n"
        "Опции:\n"
        "  --config <path>         Путь к файлу конфигурации (по умолчанию: ~/.config/synxpo/config.json)\n"
        "  --path <path>           Целевой путь для dir-pull\n"
        "  --name <name>           Имя директории для синхронизации (по умолчанию: используется id)\n\n"
        "Параметры конфигурации для 'config set':\n"
        "  server_address <адрес>     Адрес gRPC сервера (например: localhost:50051)\n"
        "  storage_path <путь>        Путь к локальному хранилищу файлов\n"
        "  backup_path <путь>         Путь для резервных копий\n"
        "  temp_path <путь>           Путь для временных файлов\n"
        "  watch_debounce_ms <мс>     Задержка отслеживания изменений в миллисекундах\n"
        "  chunk_size <байты>         Размер чанка для передачи файлов в байтах\n\n"
        "Примеры:\n"
        "  ./synxpo-client sync\n"
        "  ./synxpo-client dir-link /home/user/Documents\n"
        "  ./synxpo-client dir-pull abc123 --path /home/user/Downloads\n"
        "  ./synxpo-client dir-pull def456 --name MyProject\n"
        "  ./synxpo-client config set server_address localhost:50051\n"
        "  ./synxpo-client config set storage_path /home/user/synxpo_data\n"
        "  ./synxpo-client config set chunk_size 2097152\n"
        "  ./synxpo-client --config my_config.json config set watch_debounce_ms 1000"
    );
    std::vector<char*> args = absl::ParseCommandLine(argc, argv);
    
    std::string config_path = ExpandPath(absl::GetFlag(FLAGS_config));
    std::string target_path = ExpandPath(absl::GetFlag(FLAGS_path));
    std::string dir_name = absl::GetFlag(FLAGS_name);
    
    // Создаем директорию для конфига, если она не существует
    std::filesystem::create_directories(std::filesystem::path(config_path).parent_path());
    
    // Получаем команду из позиционных аргументов
    std::string command = "sync";  // по умолчанию
    std::vector<std::string> command_args;
    
    if (args.size() > 1) {
        command = args[1];
        for (size_t i = 2; i < args.size(); ++i) {
            command_args.push_back(args[i]);
        }
    }
    
    // Загружаем конфиг
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
    
    // Обрабатываем команды
    if (command == "dir-link") {
        if (command_args.empty()) {
            std::cerr << "Error: dir-link requires path argument\n";
            std::cerr << "Usage: dir-link <path>\n";
            return 1;
        }
        
        std::string path = command_args[0];
        synxpo::DirectoryConfig dir;
        dir.directory_id.clear();  // Будет назначен сервером
        dir.local_path = path;
        config.AddDirectory(dir);
        
        auto save_status = config.Save(config_path);
        if (!save_status.ok()) {
            std::cerr << "Failed to save config: " << save_status.message() << std::endl;
            return 1;
        }
        std::cout << "Добавлена директория для отслеживания: " << path << std::endl;
        return 0;
        
    } else if (command == "dir-pull") {
        if (command_args.empty()) {
            std::cerr << "Error: dir-pull requires id argument\n";
            std::cerr << "Usage: dir-pull <id> [--path <path>] [--name <name>]\n";
            return 1;
        }
        
        std::string id = command_args[0];
        
        std::string final_dir_name = dir_name.empty() ? id : dir_name;
        
        std::string final_path;
        if (!target_path.empty()) {
            final_path = target_path;
        } else {
            const char* home = std::getenv("HOME");
            if (home == nullptr) {
                home = getpwuid(getuid())->pw_dir;
            }
            final_path = std::string(home) + "/" + final_dir_name;
        }
        
        std::cout << "Подтягивание директории с сервера...\n";
        std::cout << "ID: " << id << "\n";
        std::cout << "Имя: " << final_dir_name << "\n";
        std::cout << "Целевой путь: " << final_path << "\n";
        
        // Создаем директорию, если она не существует
        try {
            std::filesystem::create_directories(final_path);
            std::cout << "✓ Директория создана: " << final_path << "\n";
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Error creating directory: " << e.what() << std::endl;
            return 1;
        }
        
        // Добавляем директорию в конфиг для синхронизации
        synxpo::DirectoryConfig dir;
        dir.directory_id = id;
        dir.local_path = final_path;
        config.AddDirectory(dir);
        
        auto save_status = config.Save(config_path);
        if (!save_status.ok()) {
            std::cerr << "Failed to save config: " << save_status.message() << std::endl;
            LOG_ERROR("Failed to save config: " + std::string(save_status.message()));
            return 1;
        }
        
        std::cout << "✓ Директория добавлена в конфигурацию для синхронизации\n";
        std::cout << "Теперь вы можете запустить 'sync' для синхронизации\n";
        return 0;
        
    } else if (command == "config" && command_args.size() >= 3 && command_args[0] == "set") {
        std::string key = command_args[1];
        std::string value = command_args[2];
        
        if (key == "server_address") {
            config.SetServerAddress(value);
        } else if (key == "storage_path") {
            config.SetStoragePath(value);
        } else if (key == "backup_path") {
            config.SetBackupPath(value);
        } else if (key == "temp_path") {
            config.SetTempPath(value);
        } else if (key == "watch_debounce_ms") {
            try {
                int64_t ms = std::stoll(value);
                config.SetWatchDebounce(std::chrono::milliseconds(ms));
            } catch (const std::exception& e) {
                std::cerr << "Error: invalid value for watch_debounce_ms: " << value << std::endl;
                return 1;
            }
        } else if (key == "chunk_size") {
            try {
                int64_t size = std::stoll(value);
                config.SetChunkSize(static_cast<size_t>(size));
            } catch (const std::exception& e) {
                std::cerr << "Error: invalid value for chunk_size: " << value << std::endl;
                return 1;
            }
        } else {
            std::cerr << "Error: unknown config key: " << key << std::endl;
            return 1;
        }
        
        auto save_status = config.Save(config_path);
        if (!save_status.ok()) {
            std::cerr << "Failed to save config: " << save_status.message() << std::endl;
            LOG_ERROR("Failed to save config after adding directory: " + std::string(save_status.message()));
            return 1;
        }
        std::cout << "Config updated: " << key << " = " << value << std::endl;
        return 0;
        
    } else if (command != "sync") {
        std::cerr << "Error: unknown command: " << command << std::endl;
        return 1;
    }

    // Если дошли сюда, значит команда sync или по умолчанию

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
