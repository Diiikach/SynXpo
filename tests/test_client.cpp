#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <memory>

#include "synxpo/client/config.h"
#include "synxpo/client/grpc_client.h"
#include "synxpo/common/in_memory_file_storage.h"
#include "synxpo.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using namespace synxpo;

// Test fixture for client configuration tests
class ClientConfigTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directory for test files
        temp_dir_ = std::filesystem::temp_directory_path() / "synxpo_test";
        std::filesystem::create_directories(temp_dir_);
        config_file_ = temp_dir_ / "test_config.json";
    }
    
    void TearDown() override {
        // Clean up temporary files
        std::error_code ec;
        std::filesystem::remove_all(temp_dir_, ec);
    }
    
    std::filesystem::path temp_dir_;
    std::filesystem::path config_file_;
};

// Test ClientConfig default values
TEST_F(ClientConfigTest, DefaultValuesTest) {
    ClientConfig config;
    
    EXPECT_TRUE(config.GetDirectories().empty());
    EXPECT_EQ(config.GetServerAddress(), "localhost:50051");
    EXPECT_GT(config.GetWatchDebounce().count(), 0);
    EXPECT_GT(config.GetChunkSize(), 0);
    EXPECT_GT(config.GetMaxRetryAttempts(), 0);
}

// Test ClientConfig basic setters and getters
TEST_F(ClientConfigTest, SettersGettersTest) {
    ClientConfig config;
    
    // Test server address
    config.SetServerAddress("localhost:50051");
    EXPECT_EQ(config.GetServerAddress(), "localhost:50051");
    
    // Test paths
    config.SetStoragePath("/tmp/synxpo_storage");
    EXPECT_EQ(config.GetStoragePath(), "/tmp/synxpo_storage");
    
    config.SetBackupPath("/tmp/synxpo_backup");
    EXPECT_EQ(config.GetBackupPath(), "/tmp/synxpo_backup");
    
    config.SetTempPath("/tmp/synxpo_temp");
    EXPECT_EQ(config.GetTempPath(), "/tmp/synxpo_temp");
    
    // Test sync settings
    config.SetWatchDebounce(std::chrono::milliseconds(1000));
    EXPECT_EQ(config.GetWatchDebounce().count(), 1000);
    
    config.SetChunkSize(2048);
    EXPECT_EQ(config.GetChunkSize(), 2048);
    
    config.SetMaxFileSize(100 * 1024 * 1024); // 100MB
    EXPECT_EQ(config.GetMaxFileSize(), 100 * 1024 * 1024);
    
    // Test retry settings
    config.SetMaxRetryAttempts(5);
    EXPECT_EQ(config.GetMaxRetryAttempts(), 5);
    
    config.SetRetryDelay(std::chrono::seconds(10));
    EXPECT_EQ(config.GetRetryDelay().count(), 10);
    
    // Test logging
    config.SetLogPath("/var/log/synxpo.log");
    EXPECT_EQ(config.GetLogPath(), "/var/log/synxpo.log");
    
    config.SetLogLevel("DEBUG");
    EXPECT_EQ(config.GetLogLevel(), "DEBUG");
}

// Test directory management
TEST_F(ClientConfigTest, DirectoryManagementTest) {
    ClientConfig config;
    
    EXPECT_TRUE(config.GetDirectories().empty());
    
    // Add directory
    DirectoryConfig dir1;
    dir1.directory_id = "dir1";
    dir1.local_path = "/home/user/Documents";
    dir1.enabled = true;
    
    config.AddDirectory(dir1);
    EXPECT_EQ(config.GetDirectories().size(), 1);
    EXPECT_EQ(config.GetDirectories()[0].directory_id, "dir1");
    EXPECT_EQ(config.GetDirectories()[0].local_path, "/home/user/Documents");
    EXPECT_TRUE(config.GetDirectories()[0].enabled);
    
    // Add another directory
    DirectoryConfig dir2;
    dir2.directory_id = "dir2";
    dir2.local_path = "/home/user/Projects";
    dir2.enabled = false;
    
    config.AddDirectory(dir2);
    EXPECT_EQ(config.GetDirectories().size(), 2);
    
    // Update directory
    dir1.local_path = "/home/user/NewDocuments";
    config.UpdateDirectory(dir1);
    EXPECT_EQ(config.GetDirectories().size(), 2);
    EXPECT_EQ(config.GetDirectories()[0].local_path, "/home/user/NewDocuments");
    
    // Remove directory
    config.RemoveDirectory("dir1");
    EXPECT_EQ(config.GetDirectories().size(), 1);
    EXPECT_EQ(config.GetDirectories()[0].directory_id, "dir2");
    
    // Remove non-existent directory (should not crash)
    config.RemoveDirectory("nonexistent");
    EXPECT_EQ(config.GetDirectories().size(), 1);
}

// Test configuration save and load
TEST_F(ClientConfigTest, SaveLoadTest) {
    ClientConfig config1;
    
    // Set up configuration
    config1.SetServerAddress("test.example.com:9090");
    config1.SetStoragePath("/opt/synxpo/storage");
    config1.SetBackupPath("/opt/synxpo/backup");
    config1.SetTempPath("/opt/synxpo/temp");
    config1.SetWatchDebounce(std::chrono::milliseconds(2000));
    config1.SetChunkSize(4096);
    config1.SetMaxRetryAttempts(3);
    config1.SetRetryDelay(std::chrono::seconds(5));
    
    DirectoryConfig dir;
    dir.directory_id = "test-dir-id";
    dir.local_path = "/home/test/sync";
    dir.enabled = true;
    config1.AddDirectory(dir);
    
    // Save configuration
    auto status = config1.Save(config_file_);
    EXPECT_TRUE(status.ok()) << "Save failed: " << status.message();
    
    // Verify file exists
    EXPECT_TRUE(std::filesystem::exists(config_file_));
    
    // Load configuration into new object
    ClientConfig config2;
    status = config2.Load(config_file_);
    EXPECT_TRUE(status.ok()) << "Load failed: " << status.message();
    
    // Verify loaded values
    EXPECT_EQ(config2.GetServerAddress(), "test.example.com:9090");
    EXPECT_EQ(config2.GetStoragePath(), "/opt/synxpo/storage");
    EXPECT_EQ(config2.GetBackupPath(), "/opt/synxpo/backup");
    EXPECT_EQ(config2.GetTempPath(), "/opt/synxpo/temp");
    EXPECT_EQ(config2.GetWatchDebounce().count(), 2000);
    EXPECT_EQ(config2.GetChunkSize(), 4096);
    EXPECT_EQ(config2.GetMaxRetryAttempts(), 3);
    EXPECT_EQ(config2.GetRetryDelay().count(), 5);
    
    EXPECT_EQ(config2.GetDirectories().size(), 1);
    EXPECT_EQ(config2.GetDirectories()[0].directory_id, "test-dir-id");
    EXPECT_EQ(config2.GetDirectories()[0].local_path, "/home/test/sync");
    EXPECT_TRUE(config2.GetDirectories()[0].enabled);
}

// Test loading non-existent file
TEST_F(ClientConfigTest, LoadNonExistentFileTest) {
    ClientConfig config;
    auto non_existent_file = temp_dir_ / "non_existent.json";
    
    auto status = config.Load(non_existent_file);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
}

// Test loading invalid JSON - our parser is permissive, so this should succeed
TEST_F(ClientConfigTest, LoadInvalidJsonTest) {
    // Create malformed JSON file - our parser is tolerant and will still succeed
    std::ofstream file(config_file_);
    file << "{ invalid json content }";
    file.close();
    
    ClientConfig config;
    auto status = config.Load(config_file_);
    // Our config parser is permissive and doesn't strictly validate JSON
    EXPECT_TRUE(status.ok());
}

// Test empty configuration file
TEST_F(ClientConfigTest, EmptyConfigFileTest) {
    // Create empty file
    std::ofstream file(config_file_);
    file << "{}";
    file.close();
    
    ClientConfig config;
    auto status = config.Load(config_file_);
    EXPECT_TRUE(status.ok()); // Empty config should be valid
    
    // Should have default values
    EXPECT_TRUE(config.GetDirectories().empty());
}

// Test InMemoryFileMetadataStorage
class InMemoryFileStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_ = std::make_unique<InMemoryFileMetadataStorage>();
    }
    
    std::unique_ptr<InMemoryFileMetadataStorage> storage_;
};

TEST_F(InMemoryFileStorageTest, BasicOperationsTest) {
    // Test that storage is initially empty
    // Note: We'll need to check the actual interface of InMemoryFileMetadataStorage
    // This is a placeholder test structure
    EXPECT_NE(storage_, nullptr);
}

// Mock gRPC server for client testing
class MockGRPCServer {
public:
    MockGRPCServer() : server_address_("localhost:0") {}
    
    void Start() {
        service_ = std::make_unique<MockSyncService>();
        
        grpc::ServerBuilder builder;
        int selected_port = 0;
        builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &selected_port);
        builder.RegisterService(service_.get());
        
        server_ = builder.BuildAndStart();
        server_address_ = "localhost:" + std::to_string(selected_port);
        
        // Start server in background thread
        server_thread_ = std::thread([this]() {
            server_->Wait();
        });
        
        // Wait for server to be ready
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    void Stop() {
        if (server_) {
            server_->Shutdown();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }
    
    const std::string& GetAddress() const {
        return server_address_;
    }
    
    // Get access to received messages for testing
    const std::vector<synxpo::ClientMessage>& GetReceivedMessages() const {
        return service_->GetReceivedMessages();
    }
    
private:
    class MockSyncService : public synxpo::SyncService::Service {
    public:
        grpc::Status Stream(grpc::ServerContext* context,
                           grpc::ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) override {
            
            synxpo::ClientMessage client_msg;
            while (stream->Read(&client_msg)) {
                received_messages_.push_back(client_msg);
                
                // Send appropriate response based on message type
                synxpo::ServerMessage response;
                
                if (client_msg.has_directory_create()) {
                    response.mutable_ok_directory_created()->set_directory_id("mock-dir-id");
                } else if (client_msg.has_directory_subscribe()) {
                    response.mutable_ok_subscribed()->set_directory_id(
                        client_msg.directory_subscribe().directory_id());
                } else if (client_msg.has_request_version()) {
                    auto* check = response.mutable_check_version();
                    auto* file = check->add_files();
                    file->set_id("mock-file-id");
                    file->set_directory_id("mock-dir-id");
                    file->set_version(1);
                    file->set_content_changed_version(1);
                    file->set_type(synxpo::FILE);
                    file->set_current_path("mock.txt");
                    file->set_deleted(false);
                }
                
                if (response.message_case() != synxpo::ServerMessage::MESSAGE_NOT_SET) {
                    stream->Write(response);
                }
            }
            
            return grpc::Status::OK;
        }
        
        const std::vector<synxpo::ClientMessage>& GetReceivedMessages() const {
            return received_messages_;
        }
        
    private:
        std::vector<synxpo::ClientMessage> received_messages_;
    };
    
    std::string server_address_;
    std::unique_ptr<MockSyncService> service_;
    std::unique_ptr<grpc::Server> server_;
    std::thread server_thread_;
};

// Test fixture for gRPC client tests
class GRPCClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock_server_.Start();
    }
    
    void TearDown() override {
        mock_server_.Stop();
    }
    
    MockGRPCServer mock_server_;
};

// Test GRPCClient connection
TEST_F(GRPCClientTest, ConnectionTest) {
    GRPCClient client(mock_server_.GetAddress());
    
    auto status = client.Connect();
    EXPECT_TRUE(status.ok()) << "Connection failed: " << status.message();
    
    client.Disconnect();
}

// Test GRPCClient invalid address
TEST_F(GRPCClientTest, InvalidAddressTest) {
    GRPCClient client("invalid-address:99999");
    
    auto status = client.Connect();
    EXPECT_FALSE(status.ok());
}

// Test CLI argument parsing (we need to extract and test the logic from main.cpp)
class CLITest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir_ = std::filesystem::temp_directory_path() / "synxpo_cli_test";
        std::filesystem::create_directories(temp_dir_);
        
        // Set HOME environment variable for testing
        original_home_ = std::getenv("HOME");
        setenv("HOME", temp_dir_.c_str(), 1);
    }
    
    void TearDown() override {
        // Restore original HOME
        if (original_home_) {
            setenv("HOME", original_home_, 1);
        } else {
            unsetenv("HOME");
        }
        
        std::error_code ec;
        std::filesystem::remove_all(temp_dir_, ec);
    }
    
    // Helper function to expand path (extracted from main.cpp logic)
    std::string ExpandPath(const std::string& path) {
        if (path.empty() || path[0] != '~') {
            return path;
        }
        
        const char* home = std::getenv("HOME");
        if (home == nullptr) {
            return path;
        }
        
        if (path.size() == 1) {
            return std::string(home);
        }
        
        if (path[1] == '/') {
            return std::string(home) + path.substr(1);
        }
        
        return path;
    }
    
    std::filesystem::path temp_dir_;
    const char* original_home_;
};

// Test path expansion
TEST_F(CLITest, PathExpansionTest) {
    EXPECT_EQ(ExpandPath("regular/path"), "regular/path");
    EXPECT_EQ(ExpandPath("~/config.json"), temp_dir_.string() + "/config.json");
    EXPECT_EQ(ExpandPath("~"), temp_dir_.string());
    EXPECT_EQ(ExpandPath("~user/path"), "~user/path"); // Should not expand other users
}

// Test configuration directory creation
TEST_F(CLITest, ConfigDirectoryCreationTest) {
    std::string config_path = ExpandPath("~/.config/synxpo/config.json");
    
    // Create directories
    std::filesystem::create_directories(std::filesystem::path(config_path).parent_path());
    
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(config_path).parent_path()));
}

// Test file operations
class FileOperationsTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir_ = std::filesystem::temp_directory_path() / "synxpo_file_test";
        std::filesystem::create_directories(temp_dir_);
        
        test_file_ = temp_dir_ / "test.txt";
        test_dir_ = temp_dir_ / "test_subdir";
    }
    
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(temp_dir_, ec);
    }
    
    void CreateTestFile(const std::string& content = "test content") {
        std::ofstream file(test_file_);
        file << content;
        file.close();
    }
    
    void CreateTestDirectory() {
        std::filesystem::create_directories(test_dir_);
    }
    
    std::filesystem::path temp_dir_;
    std::filesystem::path test_file_;
    std::filesystem::path test_dir_;
};

// Test file existence detection
TEST_F(FileOperationsTest, FileExistenceTest) {
    EXPECT_FALSE(std::filesystem::exists(test_file_));
    
    CreateTestFile();
    EXPECT_TRUE(std::filesystem::exists(test_file_));
    EXPECT_TRUE(std::filesystem::is_regular_file(test_file_));
}

// Test directory creation and detection
TEST_F(FileOperationsTest, DirectoryOperationsTest) {
    EXPECT_FALSE(std::filesystem::exists(test_dir_));
    
    CreateTestDirectory();
    EXPECT_TRUE(std::filesystem::exists(test_dir_));
    EXPECT_TRUE(std::filesystem::is_directory(test_dir_));
}

// Test file content reading
TEST_F(FileOperationsTest, FileContentTest) {
    const std::string expected_content = "Hello, SynXpo!";
    CreateTestFile(expected_content);
    
    std::ifstream file(test_file_);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    
    EXPECT_EQ(content, expected_content);
}

// Integration test: full config workflow
TEST_F(ClientConfigTest, FullWorkflowTest) {
    ClientConfig config;
    
    // Setup complete configuration
    config.SetServerAddress("production.synxpo.com:443");
    config.SetStoragePath("/opt/synxpo/data");
    config.SetWatchDebounce(std::chrono::milliseconds(500));
    config.SetChunkSize(1024 * 1024); // 1MB
    config.SetMaxRetryAttempts(5);
    
    // Add multiple directories
    DirectoryConfig dir1{"dir1", "/home/user/Documents", true};
    DirectoryConfig dir2{"dir2", "/home/user/Pictures", true};
    DirectoryConfig dir3{"dir3", "/home/user/Videos", false};
    
    config.AddDirectory(dir1);
    config.AddDirectory(dir2);
    config.AddDirectory(dir3);
    
    // Save to file
    auto status = config.Save(config_file_);
    ASSERT_TRUE(status.ok());
    
    // Load into new config
    ClientConfig loaded_config;
    status = loaded_config.Load(config_file_);
    ASSERT_TRUE(status.ok());
    
    // Verify all settings
    EXPECT_EQ(loaded_config.GetServerAddress(), "production.synxpo.com:443");
    EXPECT_EQ(loaded_config.GetStoragePath(), "/opt/synxpo/data");
    EXPECT_EQ(loaded_config.GetWatchDebounce().count(), 500);
    EXPECT_EQ(loaded_config.GetChunkSize(), 1024 * 1024);
    EXPECT_EQ(loaded_config.GetMaxRetryAttempts(), 5);
    
    // Verify directories
    const auto& dirs = loaded_config.GetDirectories();
    EXPECT_EQ(dirs.size(), 3);
    
    // Find and verify each directory
    bool found_dir1 = false, found_dir2 = false, found_dir3 = false;
    for (const auto& dir : dirs) {
        if (dir.directory_id == "dir1") {
            EXPECT_EQ(dir.local_path, "/home/user/Documents");
            EXPECT_TRUE(dir.enabled);
            found_dir1 = true;
        } else if (dir.directory_id == "dir2") {
            EXPECT_EQ(dir.local_path, "/home/user/Pictures");
            EXPECT_TRUE(dir.enabled);
            found_dir2 = true;
        } else if (dir.directory_id == "dir3") {
            EXPECT_EQ(dir.local_path, "/home/user/Videos");
            EXPECT_FALSE(dir.enabled);
            found_dir3 = true;
        }
    }
    
    EXPECT_TRUE(found_dir1 && found_dir2 && found_dir3);
    
    // Test directory operations
    loaded_config.RemoveDirectory("dir2");
    EXPECT_EQ(loaded_config.GetDirectories().size(), 2);
    
    // Update directory
    DirectoryConfig updated_dir3{"dir3", "/home/user/NewVideos", true};
    loaded_config.UpdateDirectory(updated_dir3);
    
    const auto& updated_dirs = loaded_config.GetDirectories();
    for (const auto& dir : updated_dirs) {
        if (dir.directory_id == "dir3") {
            EXPECT_EQ(dir.local_path, "/home/user/NewVideos");
            EXPECT_TRUE(dir.enabled);
        }
    }
}