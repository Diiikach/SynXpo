#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace synxpo::staging_fs {

// Infrastructure adapter: files are addressed relative to an isolated staging root.
class FilesystemClient {
public:
    explicit FilesystemClient(std::filesystem::path root) : root_(std::move(root)) {
        std::filesystem::create_directories(root_);
    }

    [[nodiscard]] std::filesystem::path PathFor(
        std::string_view session_id, std::string_view file_id) const {
        return root_ / SafePart(session_id) / SafePart(file_id);
    }

    // Writes exactly at offset; the returned value is the next resume offset.
    [[nodiscard]] std::uint64_t Write(
        std::string_view session_id, std::string_view file_id,
        std::uint64_t offset, std::span<const std::byte> bytes) const {
        const auto path = PathFor(session_id, file_id);
        std::filesystem::create_directories(path.parent_path());

        std::fstream stream(path, std::ios::binary | std::ios::in | std::ios::out);
        if (!stream) {  // First chunk: create the file, then reopen it read/write.
            std::ofstream(path, std::ios::binary).close();
            stream.open(path, std::ios::binary | std::ios::in | std::ios::out);
        }
        if (!stream) throw std::runtime_error("cannot open staging file");

        stream.seekp(static_cast<std::streamoff>(offset));
        stream.write(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<std::streamsize>(bytes.size()));
        if (!stream) throw std::runtime_error("cannot write staging file");
        return offset + bytes.size();
    }

    [[nodiscard]] std::uint64_t Size(
        std::string_view session_id, std::string_view file_id) const {
        const auto path = PathFor(session_id, file_id);
        return std::filesystem::exists(path) ? std::filesystem::file_size(path) : 0;
    }

    void RemoveSession(std::string_view session_id) const {
        std::filesystem::remove_all(root_ / SafePart(session_id));
    }

private:
    static std::filesystem::path SafePart(std::string_view value) {
        const std::filesystem::path path{std::string(value)};
        if (value.empty() || path.is_absolute() || path.has_parent_path() || value == "." || value == "..") {
            throw std::invalid_argument("staging identifier must be one path component");
        }
        return path;
    }

    std::filesystem::path root_;
};

}  // namespace synxpo::staging_fs
