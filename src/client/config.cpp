#include "synxpo/client/config.h"

#include <algorithm>
 #include <cctype>
 #include <fstream>
 #include <optional>
 #include <regex>
 #include <sstream>
 #include <string_view>

namespace synxpo {

namespace {

std::string JsonEscape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
        case '\\': out += "\\\\"; break;
        case '"': out += "\\\""; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            out += c;
            break;
        }
    }
    return out;
}

std::optional<std::string> ExtractJsonStringField(const std::string& text, std::string_view key) {
    std::string pattern;
    pattern.reserve(key.size() + 16);
    pattern += "\"";
    pattern += key;
    pattern += "\"";
    pattern += "\\s*:\\s*\"([^\"]*)\"";

    const std::regex re(pattern);
    std::smatch m;
    if (!std::regex_search(text, m, re) || m.size() < 2) {
        return std::nullopt;
    }
    return m[1].str();
}

std::optional<long long> ExtractJsonIntField(const std::string& text, std::string_view key) {
    std::string pattern;
    pattern.reserve(key.size() + 16);
    pattern += "\"";
    pattern += key;
    pattern += "\"";
    pattern += "\\s*:\\s*([0-9]+)";

    const std::regex re(pattern);
    std::smatch m;
    if (!std::regex_search(text, m, re) || m.size() < 2) {
        return std::nullopt;
    }
    return std::stoll(m[1].str());
}

std::optional<bool> ExtractJsonBoolField(const std::string& text, std::string_view key) {
    std::string pattern;
    pattern.reserve(key.size() + 24);
    pattern += "\"";
    pattern += key;
    pattern += "\"";
    pattern += "\\s*:\\s*(true|false)";

    const std::regex re(pattern);
    std::smatch m;
    if (!std::regex_search(text, m, re) || m.size() < 2) {
        return std::nullopt;
    }
    return m[1].str() == "true";
}

std::vector<DirectoryConfig> ParseDirectories(const std::string& text) {
    std::vector<DirectoryConfig> out;

    const std::regex array_re("\\\"directories\\\"\\s*:\\s*\\[([\\s\\S]*)\\]", std::regex::ECMAScript);
    std::smatch array_m;
    if (!std::regex_search(text, array_m, array_re) || array_m.size() < 2) {
        return out;
    }

    const std::string array_body = array_m[1].str();
    const std::regex obj_re("\\{([^}]*)\\}");
    auto begin = std::sregex_iterator(array_body.begin(), array_body.end(), obj_re);
    auto end = std::sregex_iterator();

    for (auto it = begin; it != end; ++it) {
        const std::string obj = (*it)[1].str();
        DirectoryConfig dir;

        if (auto v = ExtractJsonStringField(obj, "directory_id")) {
            dir.directory_id = *v;
        }
        if (auto v = ExtractJsonStringField(obj, "local_path")) {
            dir.local_path = *v;
        }
        if (auto v = ExtractJsonBoolField(obj, "enabled")) {
            dir.enabled = *v;
        }

        if (!dir.local_path.empty()) {
            out.push_back(std::move(dir));
        }
    }

    return out;
}

}  // namespace

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
    std::ifstream in(config_file);
    if (!in.is_open()) {
        return absl::NotFoundError("Config file not found");
    }

    std::ostringstream ss;
    ss << in.rdbuf();
    const std::string text = ss.str();

    if (auto v = ExtractJsonStringField(text, "server_address")) {
        server_address_ = *v;
    }
    if (auto v = ExtractJsonStringField(text, "storage_path")) {
        storage_path_ = *v;
    }
    if (auto v = ExtractJsonStringField(text, "backup_path")) {
        backup_path_ = *v;
    }
    if (auto v = ExtractJsonStringField(text, "temp_path")) {
        temp_path_ = *v;
    }
    if (auto v = ExtractJsonIntField(text, "watch_debounce_ms")) {
        watch_debounce_ = std::chrono::milliseconds(*v);
    }
    if (auto v = ExtractJsonIntField(text, "max_file_size")) {
        max_file_size_ = static_cast<size_t>(*v);
    }
    if (auto v = ExtractJsonIntField(text, "chunk_size")) {
        chunk_size_ = static_cast<size_t>(*v);
    }
    if (auto v = ExtractJsonIntField(text, "max_retry_attempts")) {
        max_retry_attempts_ = static_cast<int>(*v);
    }
    if (auto v = ExtractJsonIntField(text, "retry_delay_s")) {
        retry_delay_ = std::chrono::seconds(*v);
    }
    if (auto v = ExtractJsonStringField(text, "log_path")) {
        log_path_ = *v;
    }
    if (auto v = ExtractJsonStringField(text, "log_level")) {
        log_level_ = *v;
    }

    directories_ = ParseDirectories(text);
    return absl::OkStatus();
}

absl::Status ClientConfig::Save(const std::filesystem::path& config_file) const {
    std::error_code ec;
    auto parent = config_file.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            return absl::InternalError("Failed to create config directory");
        }
    }

    std::ofstream out(config_file, std::ios::trunc);
    if (!out.is_open()) {
        return absl::InternalError("Failed to open config file for writing");
    }

    out << "{\n";
    out << "  \"server_address\": \"" << JsonEscape(server_address_) << "\",\n";
    out << "  \"storage_path\": \"" << JsonEscape(storage_path_.string()) << "\",\n";
    out << "  \"backup_path\": \"" << JsonEscape(backup_path_.string()) << "\",\n";
    out << "  \"temp_path\": \"" << JsonEscape(temp_path_.string()) << "\",\n";
    out << "  \"watch_debounce_ms\": " << watch_debounce_.count() << ",\n";
    out << "  \"max_file_size\": " << max_file_size_ << ",\n";
    out << "  \"chunk_size\": " << chunk_size_ << ",\n";
    out << "  \"max_retry_attempts\": " << max_retry_attempts_ << ",\n";
    out << "  \"retry_delay_s\": " << retry_delay_.count() << ",\n";
    out << "  \"log_path\": \"" << JsonEscape(log_path_.string()) << "\",\n";
    out << "  \"log_level\": \"" << JsonEscape(log_level_) << "\",\n";
    out << "  \"directories\": [\n";

    for (size_t i = 0; i < directories_.size(); ++i) {
        const auto& dir = directories_[i];
        out << "    {\n";
        out << "      \"directory_id\": \"" << JsonEscape(dir.directory_id) << "\",\n";
        out << "      \"local_path\": \"" << JsonEscape(dir.local_path.string()) << "\",\n";
        out << "      \"enabled\": " << (dir.enabled ? "true" : "false") << "\n";
        out << "    }";
        if (i + 1 != directories_.size()) {
            out << ",";
        }
        out << "\n";
    }

    out << "  ]\n";
    out << "}\n";
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
    // First try to find by directory_id if it's not empty
    if (!dir.directory_id.empty()) {
        auto it = std::find_if(directories_.begin(), directories_.end(),
            [&dir](const DirectoryConfig& d) {
                return d.directory_id == dir.directory_id;
            });
        
        if (it != directories_.end()) {
            *it = dir;
            return;
        }
    }
    
    // Otherwise find by local_path
    auto it = std::find_if(directories_.begin(), directories_.end(),
        [&dir](const DirectoryConfig& d) {
            return d.local_path == dir.local_path;
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
