#include "synxpo/client/config.h"

#include <algorithm>

namespace synxpo {

ClientConfig::ClientConfig()
    : server_address_("localhost:50051"),
      storage_path_("~/.synxpo/storage"),
      backup_path_("~/.synxpo/backups"),
      temp_path_("~/.synxpo/temp"),
      watch_debounce_(100),
      max_file_size_(100 * 1024 * 1024),  // 100 MB
      chunk_size_(64 * 1024),              // 64 KB
      max_retry_attempts_(3),
      retry_delay_(5),
      log_path_("~/.synxpo/client.log"),
      log_level_("info") {
}

ClientConfig::~ClientConfig() = default;

absl::Status ClientConfig::Load(const std::filesystem::path& config_file) {
    // TODO: Implement loading from file (JSON/TOML)
    return absl::OkStatus();
}

absl::Status ClientConfig::Save(const std::filesystem::path& config_file) const {
    // TODO: Implement saving to file (JSON/TOML)
    return absl::OkStatus();
}

void ClientConfig::AddDirectory(const DirectoryConfig& dir) {
    directories_.push_back(dir);
}

void ClientConfig::RemoveDirectory(const std::string& directory_id) {
    directories_.erase(
        std::remove_if(directories_.begin(), directories_.end(),
            [&directory_id](const DirectoryConfig& dir) {
                return dir.directory_id == directory_id;
            }),
        directories_.end());
}

void ClientConfig::UpdateDirectory(const DirectoryConfig& dir) {
    auto it = std::find_if(directories_.begin(), directories_.end(),
        [&dir](const DirectoryConfig& d) {
            return d.directory_id == dir.directory_id;
        });
    
    if (it != directories_.end()) {
        *it = dir;
    }
}

const std::vector<DirectoryConfig>& ClientConfig::GetDirectories() const {
    return directories_;
}

void ClientConfig::SetServerAddress(const std::string& address) {
    server_address_ = address;
}

const std::string& ClientConfig::GetServerAddress() const {
    return server_address_;
}

void ClientConfig::SetStoragePath(const std::filesystem::path& path) {
    storage_path_ = path;
}

const std::filesystem::path& ClientConfig::GetStoragePath() const {
    return storage_path_;
}

void ClientConfig::SetBackupPath(const std::filesystem::path& path) {
    backup_path_ = path;
}

const std::filesystem::path& ClientConfig::GetBackupPath() const {
    return backup_path_;
}

void ClientConfig::SetTempPath(const std::filesystem::path& path) {
    temp_path_ = path;
}

const std::filesystem::path& ClientConfig::GetTempPath() const {
    return temp_path_;
}

void ClientConfig::SetWatchDebounce(std::chrono::milliseconds debounce) {
    watch_debounce_ = debounce;
}

std::chrono::milliseconds ClientConfig::GetWatchDebounce() const {
    return watch_debounce_;
}

void ClientConfig::SetMaxFileSize(size_t size) {
    max_file_size_ = size;
}

size_t ClientConfig::GetMaxFileSize() const {
    return max_file_size_;
}

void ClientConfig::SetChunkSize(size_t size) {
    chunk_size_ = size;
}

size_t ClientConfig::GetChunkSize() const {
    return chunk_size_;
}

void ClientConfig::SetMaxRetryAttempts(int attempts) {
    max_retry_attempts_ = attempts;
}

int ClientConfig::GetMaxRetryAttempts() const {
    return max_retry_attempts_;
}

void ClientConfig::SetRetryDelay(std::chrono::seconds delay) {
    retry_delay_ = delay;
}

std::chrono::seconds ClientConfig::GetRetryDelay() const {
    return retry_delay_;
}

void ClientConfig::SetLogPath(const std::filesystem::path& path) {
    log_path_ = path;
}

const std::filesystem::path& ClientConfig::GetLogPath() const {
    return log_path_;
}

void ClientConfig::SetLogLevel(const std::string& level) {
    log_level_ = level;
}

const std::string& ClientConfig::GetLogLevel() const {
    return log_level_;
}

}  // namespace synxpo
