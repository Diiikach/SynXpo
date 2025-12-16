#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <absl/status/status.h>

#include "synxpo.pb.h"
#include "synxpo/client/config.h"
#include "synxpo/client/file_watcher.h"
#include "synxpo/client/grpc_client.h"
#include "synxpo/common/file_storage.h"

namespace synxpo {

class Synchronizer {
public:
    Synchronizer(
        ClientConfig& config,
        IFileMetadataStorage& storage,
        GRPCClient& grpc_client,
        FileWatcher& file_watcher);
    
    ~Synchronizer();

    Synchronizer(const Synchronizer&) = delete;
    Synchronizer& operator=(const Synchronizer&) = delete;
    Synchronizer(Synchronizer&&) = delete;
    Synchronizer& operator=(Synchronizer&&) = delete;

    // Start automatic background synchronization
    absl::Status StartAutoSync();
    
    // Stop automatic synchronization
    void StopAutoSync();
    
    bool IsAutoSyncRunning() const;
    
    // One-time synchronization of all directories
    absl::Status SyncOnce();
    
    // One-time synchronization of specific directory
    absl::Status SyncDirectory(const std::string& directory_id);

private:
    struct FileChangeInfo {
        std::optional<std::string> file_id;  // nullopt for new files
        std::string directory_id;
        std::filesystem::path current_path;
        bool deleted;
        bool content_changed;
        std::chrono::system_clock::time_point first_try_time;
    };

    // Initialize all directories from config
    absl::Status InitializeDirectories();
    
    // Create new directory
    absl::Status CreateNewDirectory(DirectoryConfig& dir);
    
    // Upload initial files for new directory
    absl::Status UploadInitialFiles(const DirectoryConfig& dir);
    
    // Subscribe to existing directory
    absl::Status SubscribeToDirectory(const std::string& directory_id);
    
    // Handle file event from FileWatcher
    void OnFileEvent(const FileEvent& event);
    
    // Send ASK_VERSION_INCREASE request
    absl::Status AskVersionIncrease(
        const std::string& directory_id,
        const std::vector<FileChangeInfo>& changes);
    
    // Upload file contents after VERSION_INCREASE_ALLOW
    absl::Status UploadFileContents(
        const std::string& directory_id,
        const std::vector<FileChangeInfo>& files);
    
    // Handle VERSION_INCREASE_DENY response
    absl::Status HandleVersionIncreaseDeny(
        const std::string& directory_id,
        const std::vector<FileStatusInfo>& file_statuses);
    
    // Request file versions (REQUEST_VERSION)
    absl::Status RequestVersions(const std::string& directory_id);
    
    // Request versions for specific files (for DENIED files)
    absl::Status RequestFileVersions(
        const std::string& directory_id,
        const std::vector<std::string>& file_ids);
    
    // Process CHECK_VERSION - compare and synchronize
    absl::Status ProcessCheckVersion(
        const std::string& directory_id,
        const std::vector<FileMetadata>& server_files);
    
    // Calculate differences between local and server versions
    struct VersionDiff {
        std::vector<FileMetadata> to_download;
        std::vector<FileMetadata> to_rename_delete;
        std::vector<FileMetadata> to_upload;
        std::vector<std::string> to_delete_local;
    };
    VersionDiff CalculateVersionDiff(
        const std::string& directory_id,
        const std::vector<FileMetadata>& server_files);
    
    // Apply renames and deletions
    absl::Status ApplyRenamesAndDeletes(
        const std::string& directory_id,
        const std::vector<FileMetadata>& files);
    
    // Request file contents (REQUEST_FILE_CONTENT)
    absl::Status RequestFileContents(
        const std::string& directory_id,
        const std::vector<FileMetadata>& files);
    
    // Receive and write file contents (FILE_WRITE stream)
    absl::Status ReceiveAndWriteFiles(
        const std::string& directory_id,
        const std::vector<FileMetadata>& files);
    
    // Handle FILE_CONTENT_REQUEST_DENY response
    absl::Status HandleFileContentRequestDeny(
        const std::string& directory_id,
        const std::vector<FileStatusInfo>& file_statuses);
    
    // Delete local files missing on server
    absl::Status DeleteMissingFiles(
        const std::string& directory_id,
        const std::vector<std::string>& file_ids);
    
    // Server message handlers
    void OnServerMessage(const ServerMessage& message);
    void HandleOkDirectoryCreated(const OkDirectoryCreated& msg);
    void HandleOkSubscribed(const OkSubscribed& msg);
    void HandleVersionIncreaseAllow(const VersionIncreaseAllow& msg);
    void HandleVersionIncreaseDeny(const VersionIncreaseDeny& msg);
    void HandleVersionIncreased(const VersionIncreased& msg);
    void HandleCheckVersion(const CheckVersion& msg);
    void HandleFileContentRequestAllow(const FileContentRequestAllow& msg);
    void HandleFileContentRequestDeny(const FileContentRequestDeny& msg);
    void HandleFileWrite(const FileWrite& msg);
    void HandleFileWriteEnd(const FileWriteEnd& msg);
    void HandleError(const Error& msg);
    
    // Backup stubs - TODO: implement via IBackupManager
    absl::Status BackupFile(const std::filesystem::path& file_path);
    absl::Status RestoreFile(const std::filesystem::path& file_path);
    void CleanupBackups();
    
    // Convert FileEvent to FileChangeInfo
    FileChangeInfo EventToChangeInfo(const FileEvent& event);
    
    // Get directory path by directory_id
    std::optional<std::filesystem::path> GetDirectoryPath(const std::string& directory_id) const;

    // Process pending changes for a directory
    void ProcessPendingChanges(const std::string& directory_id);

private:
    struct DirectoryState {
        bool subscribed = false;
        bool is_syncing = false;
        std::set<std::string> blocked_files;  // Files with BLOCKED status
        std::map<std::filesystem::path, FileChangeInfo> pending_changes;
        std::set<std::filesystem::path> files_being_written;  // Files currently being written by sync
        
        std::chrono::system_clock::time_point last_change_time;
    };
    
    struct FileTransferState {
        bool active = false;
        std::string directory_id;
        std::vector<FileMetadata> files;
        
        // For writing during download
        std::map<std::string, std::ofstream> write_streams;
        std::map<std::string, std::filesystem::path> temp_paths;
        std::map<std::string, std::filesystem::path> final_paths;
        
        // For reading during upload
        std::map<std::string, std::ifstream> read_streams;
        
        std::chrono::system_clock::time_point last_activity;
    };
    
    ClientConfig& config_;
    IFileMetadataStorage& storage_;
    GRPCClient& grpc_client_;
    FileWatcher& file_watcher_;
    
    std::atomic<bool> auto_sync_running_{false};
    std::atomic<bool> debounce_thread_running_{false};
    std::thread debounce_thread_;
    
    mutable std::mutex state_mutex_;
    std::unordered_map<std::string, DirectoryState> directory_states_;
    
    mutable std::mutex transfer_mutex_;
    FileTransferState upload_state_;
    FileTransferState download_state_;
    
    mutable std::mutex sync_mutex_;
};

}  // namespace synxpo
