#include <gtest/gtest.h>
#include "synxpo.pb.h"
#include "synxpo.grpc.pb.h"
#include <google/protobuf/util/message_differencer.h>

class ProtobufMessageTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code here
    }
    
    void TearDown() override {
        // Cleanup code here
    }
};

// Test FileMetadata message creation and serialization
TEST_F(ProtobufMessageTest, FileMetadataTest) {
    synxpo::FileMetadata file;
    file.set_id("test-file-id");
    file.set_directory_id("test-dir-id");
    file.set_version(1);
    file.set_content_changed_version(1);
    file.set_type(synxpo::FILE);
    file.set_current_path("test.txt");
    file.set_deleted(false);
    
    EXPECT_EQ(file.id(), "test-file-id");
    EXPECT_EQ(file.directory_id(), "test-dir-id");
    EXPECT_EQ(file.version(), 1);
    EXPECT_EQ(file.content_changed_version(), 1);
    EXPECT_EQ(file.type(), synxpo::FILE);
    EXPECT_EQ(file.current_path(), "test.txt");
    EXPECT_FALSE(file.deleted());
}

// Test FileStatusInfo message
TEST_F(ProtobufMessageTest, FileStatusInfoTest) {
    synxpo::FileStatusInfo status;
    status.set_id("file-id");
    status.set_directory_id("dir-id");
    status.set_status(synxpo::BLOCKED);
    
    EXPECT_EQ(status.id(), "file-id");
    EXPECT_EQ(status.directory_id(), "dir-id");
    EXPECT_EQ(status.status(), synxpo::BLOCKED);
}

// Test FileChunk message
TEST_F(ProtobufMessageTest, FileChunkTest) {
    synxpo::FileChunk chunk;
    chunk.set_id("chunk-file-id");
    chunk.set_directory_id("chunk-dir-id");
    chunk.set_data("Hello, World!");
    chunk.set_offset(0);
    
    EXPECT_EQ(chunk.id(), "chunk-file-id");
    EXPECT_EQ(chunk.directory_id(), "chunk-dir-id");
    EXPECT_EQ(chunk.data(), "Hello, World!");
    EXPECT_EQ(chunk.offset(), 0);
}

// Test DirectoryCreate client message
TEST_F(ProtobufMessageTest, DirectoryCreateMessageTest) {
    synxpo::ClientMessage msg;
    msg.set_request_id("request-123");
    msg.mutable_directory_create();
    
    EXPECT_EQ(msg.request_id(), "request-123");
    EXPECT_TRUE(msg.has_directory_create());
    EXPECT_FALSE(msg.has_directory_subscribe());
}

// Test DirectorySubscribe client message
TEST_F(ProtobufMessageTest, DirectorySubscribeMessageTest) {
    synxpo::ClientMessage msg;
    msg.set_request_id("request-456");
    auto* subscribe = msg.mutable_directory_subscribe();
    subscribe->set_directory_id("dir-to-subscribe");
    
    EXPECT_EQ(msg.request_id(), "request-456");
    EXPECT_TRUE(msg.has_directory_subscribe());
    EXPECT_EQ(msg.directory_subscribe().directory_id(), "dir-to-subscribe");
}

// Test AskVersionIncrease message
TEST_F(ProtobufMessageTest, AskVersionIncreaseMessageTest) {
    synxpo::ClientMessage msg;
    auto* ask = msg.mutable_ask_version_increase();
    
    auto* file_info = ask->add_files();
    file_info->set_id("file-id-1");
    file_info->set_directory_id("dir-id-1");
    file_info->set_current_path("path/to/file.txt");
    file_info->set_deleted(false);
    file_info->set_content_changed(true);
    file_info->set_type(synxpo::FILE);
    
    EXPECT_TRUE(msg.has_ask_version_increase());
    EXPECT_EQ(ask->files_size(), 1);
    EXPECT_EQ(ask->files(0).id(), "file-id-1");
    EXPECT_EQ(ask->files(0).directory_id(), "dir-id-1");
    EXPECT_EQ(ask->files(0).current_path(), "path/to/file.txt");
    EXPECT_FALSE(ask->files(0).deleted());
    EXPECT_TRUE(ask->files(0).content_changed());
    EXPECT_EQ(ask->files(0).type(), synxpo::FILE);
}

// Test RequestVersion message
TEST_F(ProtobufMessageTest, RequestVersionMessageTest) {
    synxpo::ClientMessage msg;
    auto* request = msg.mutable_request_version();
    
    // Test directory request
    auto* dir_request = request->add_requests();
    dir_request->set_directory_id("dir-123");
    
    // Test specific file request
    auto* file_request = request->add_requests();
    auto* file_id = file_request->mutable_file_id();
    file_id->set_id("file-456");
    file_id->set_directory_id("dir-789");
    
    EXPECT_TRUE(msg.has_request_version());
    EXPECT_EQ(request->requests_size(), 2);
    EXPECT_EQ(request->requests(0).directory_id(), "dir-123");
    EXPECT_TRUE(request->requests(1).has_file_id());
    EXPECT_EQ(request->requests(1).file_id().id(), "file-456");
    EXPECT_EQ(request->requests(1).file_id().directory_id(), "dir-789");
}

// Test OkDirectoryCreated server message
TEST_F(ProtobufMessageTest, OkDirectoryCreatedMessageTest) {
    synxpo::ServerMessage msg;
    msg.set_request_id("response-123");
    auto* ok = msg.mutable_ok_directory_created();
    ok->set_directory_id("new-dir-id");
    
    EXPECT_EQ(msg.request_id(), "response-123");
    EXPECT_TRUE(msg.has_ok_directory_created());
    EXPECT_EQ(msg.ok_directory_created().directory_id(), "new-dir-id");
}

// Test CheckVersion server message
TEST_F(ProtobufMessageTest, CheckVersionMessageTest) {
    synxpo::ServerMessage msg;
    auto* check = msg.mutable_check_version();
    
    auto* file1 = check->add_files();
    file1->set_id("file-1");
    file1->set_directory_id("dir-1");
    file1->set_version(10);
    file1->set_content_changed_version(5);
    file1->set_type(synxpo::FILE);
    file1->set_current_path("test.txt");
    file1->set_deleted(false);
    
    auto* file2 = check->add_files();
    file2->set_id("file-2");
    file2->set_directory_id("dir-1");
    file2->set_version(3);
    file2->set_content_changed_version(2);
    file2->set_type(synxpo::FOLDER);
    file2->set_current_path("subfolder");
    file2->set_deleted(true);
    
    EXPECT_TRUE(msg.has_check_version());
    EXPECT_EQ(check->files_size(), 2);
    
    EXPECT_EQ(check->files(0).id(), "file-1");
    EXPECT_EQ(check->files(0).version(), 10);
    EXPECT_EQ(check->files(0).type(), synxpo::FILE);
    EXPECT_FALSE(check->files(0).deleted());
    
    EXPECT_EQ(check->files(1).id(), "file-2");
    EXPECT_EQ(check->files(1).version(), 3);
    EXPECT_EQ(check->files(1).type(), synxpo::FOLDER);
    EXPECT_TRUE(check->files(1).deleted());
}

// Test Error server message
TEST_F(ProtobufMessageTest, ErrorMessageTest) {
    synxpo::ServerMessage msg;
    auto* error = msg.mutable_error();
    error->set_code(synxpo::Error::FILE_NOT_FOUND);
    error->set_message("File not found on server");
    error->add_file_ids("missing-file-1");
    error->add_file_ids("missing-file-2");
    
    EXPECT_TRUE(msg.has_error());
    EXPECT_EQ(error->code(), synxpo::Error::FILE_NOT_FOUND);
    EXPECT_EQ(error->message(), "File not found on server");
    EXPECT_EQ(error->file_ids_size(), 2);
    EXPECT_EQ(error->file_ids(0), "missing-file-1");
    EXPECT_EQ(error->file_ids(1), "missing-file-2");
}

// Test message serialization and deserialization
TEST_F(ProtobufMessageTest, SerializationTest) {
    // Create original message
    synxpo::ClientMessage original;
    original.set_request_id("serialize-test");
    auto* subscribe = original.mutable_directory_subscribe();
    subscribe->set_directory_id("serialize-dir");
    
    // Serialize
    std::string serialized;
    EXPECT_TRUE(original.SerializeToString(&serialized));
    EXPECT_GT(serialized.size(), 0);
    
    // Deserialize
    synxpo::ClientMessage deserialized;
    EXPECT_TRUE(deserialized.ParseFromString(serialized));
    
    // Compare
    EXPECT_EQ(original.request_id(), deserialized.request_id());
    EXPECT_TRUE(deserialized.has_directory_subscribe());
    EXPECT_EQ(original.directory_subscribe().directory_id(), 
              deserialized.directory_subscribe().directory_id());
}

// Test FileWrite messages
TEST_F(ProtobufMessageTest, FileWriteMessageTest) {
    synxpo::ClientMessage client_msg;
    auto* write = client_msg.mutable_file_write();
    auto* chunk = write->mutable_chunk();
    chunk->set_id("write-file-id");
    chunk->set_directory_id("write-dir-id");
    chunk->set_data("binary data chunk");
    chunk->set_offset(1024);
    
    EXPECT_TRUE(client_msg.has_file_write());
    EXPECT_EQ(chunk->id(), "write-file-id");
    EXPECT_EQ(chunk->directory_id(), "write-dir-id");
    EXPECT_EQ(chunk->data(), "binary data chunk");
    EXPECT_EQ(chunk->offset(), 1024);
    
    // Test FileWriteEnd
    synxpo::ClientMessage end_msg;
    end_msg.mutable_file_write_end();
    EXPECT_TRUE(end_msg.has_file_write_end());
}