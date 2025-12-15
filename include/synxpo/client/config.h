#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

#include <absl/status/status.h>

namespace synxpo {

struct DirectoryConfig {
    std::string directory_id;
    std::filesystem::path local_path;
    bool enabled = true;
};

class ClientConfig {
public:
    ClientConfig();
    ~ClientConfig();

    ClientConfig(const ClientConfig&) = delete;
    ClientConfig& operator=(const ClientConfig&) = delete;
    ClientConfig(ClientConfig&&) = delete;
    ClientConfig& operator=(ClientConfig&&) = delete;

    // Load configuration from file
    absl::Status Load(const std::filesystem::path& config_file);
    
    // Save configuration to file
    absl::Status Save(const std::filesystem::path& config_file) const;

    // Directory management
    void AddDirectory(const DirectoryConfig& dir);
    void RemoveDirectory(const std::string& directory_id);
    void UpdateDirectory(const DirectoryConfig& dir);
    const std::vector<DirectoryConfig>& GetDirectories() const;

    // Server settings
    void SetServerAddress(const std::string& address);
    const std::string& GetServerAddress() const;

    // Path settings
    void SetStoragePath(const std::filesystem::path& path);
    const std::filesystem::path& GetStoragePath() const;
    
    void SetBackupPath(const std::filesystem::path& path);
    const std::filesystem::path& GetBackupPath() const;
    
    void SetTempPath(const std::filesystem::path& path);
    const std::filesystem::path& GetTempPath() const;

    // Sync settings
    void SetWatchDebounce(std::chrono::milliseconds debounce);
    std::chrono::milliseconds GetWatchDebounce() const;
    
    void SetMaxFileSize(size_t size);
    size_t GetMaxFileSize() const;
    
    void SetChunkSize(size_t size);
    size_t GetChunkSize() const;

    // Retry settings
    void SetMaxRetryAttempts(int attempts);
    int GetMaxRetryAttempts() const;
    
    void SetRetryDelay(std::chrono::seconds delay);
    std::chrono::seconds GetRetryDelay() const;

    // Logging
    void SetLogPath(const std::filesystem::path& path);
    const std::filesystem::path& GetLogPath() const;
    
    void SetLogLevel(const std::string& level);
    const std::string& GetLogLevel() const;

private:
    std::vector<DirectoryConfig> directories_;
    
    std::string server_address_;
    
    std::filesystem::path storage_path_;
    std::filesystem::path backup_path_;
    std::filesystem::path temp_path_;
    
    std::chrono::milliseconds watch_debounce_;
    size_t max_file_size_;
    size_t chunk_size_;
    
    int max_retry_attempts_;
    std::chrono::seconds retry_delay_;
    
    std::filesystem::path log_path_;
    std::string log_level_;
};

}  // namespace synxpo
