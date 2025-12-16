#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <memory>
#include <chrono>
#include <queue>

#include "synxpo.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

// Mock implementation of SyncService for testing
class MockSyncServiceImpl : public synxpo::SyncService::Service {
private:
    std::queue<synxpo::ServerMessage> prepared_responses_;
    std::vector<synxpo::ClientMessage> received_messages_;
    
public:
    void PrepareResponse(const synxpo::ServerMessage& response) {
        prepared_responses_.push(response);
    }
    
    const std::vector<synxpo::ClientMessage>& GetReceivedMessages() const {
        return received_messages_;
    }
    
    void ClearReceivedMessages() {
        received_messages_.clear();
    }
    
    Status Stream(ServerContext* context,
                  ServerReaderWriter<synxpo::ServerMessage, synxpo::ClientMessage>* stream) override {
        
        synxpo::ClientMessage client_msg;
        
        while (stream->Read(&client_msg)) {
            received_messages_.push_back(client_msg);
            
            // Send prepared response if available
            if (!prepared_responses_.empty()) {
                auto response = prepared_responses_.front();
                prepared_responses_.pop();
                
                if (!stream->Write(response)) {
                    return Status::CANCELLED;
                }
            }
        }
        
        return Status::OK;
    }
};

class ServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Start server on random port
        server_address_ = "localhost:0";
        service_ = std::make_unique<MockSyncServiceImpl>();
        
        ServerBuilder builder;
        int selected_port = 0;
        builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &selected_port);
        builder.RegisterService(service_.get());
        
        server_ = builder.BuildAndStart();
        server_address_ = "localhost:" + std::to_string(selected_port);
        
        // Create client
        auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
        stub_ = synxpo::SyncService::NewStub(channel);
        
        // Wait for server to be ready
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    void TearDown() override {
        if (server_) {
            server_->Shutdown();
        }
    }
    
    std::string server_address_;
    std::unique_ptr<MockSyncServiceImpl> service_;
    std::unique_ptr<Server> server_;
    std::unique_ptr<synxpo::SyncService::Stub> stub_;
};

// Test server startup and connection
TEST_F(ServerTest, ServerStartupTest) {
    EXPECT_NE(server_, nullptr);
    EXPECT_NE(stub_, nullptr);
    
    // Try to create a stream connection
    ClientContext context;
    auto stream = stub_->Stream(&context);
    EXPECT_NE(stream, nullptr);
    
    // Close the stream
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
}

// Test DirectoryCreate message handling
TEST_F(ServerTest, DirectoryCreateTest) {
    // Prepare server response
    synxpo::ServerMessage response;
    response.mutable_ok_directory_created()->set_directory_id("test-dir-123");
    service_->PrepareResponse(response);
    
    // Create client stream
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send DirectoryCreate request
    synxpo::ClientMessage request;
    request.set_request_id("req-123");
    request.mutable_directory_create();
    
    EXPECT_TRUE(stream->Write(request));
    
    // Read response
    synxpo::ServerMessage server_response;
    EXPECT_TRUE(stream->Read(&server_response));
    
    EXPECT_TRUE(server_response.has_ok_directory_created());
    EXPECT_EQ(server_response.ok_directory_created().directory_id(), "test-dir-123");
    
    // Close stream
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
    
    // Check that server received our message
    const auto& received = service_->GetReceivedMessages();
    ASSERT_EQ(received.size(), 1);
    EXPECT_EQ(received[0].request_id(), "req-123");
    EXPECT_TRUE(received[0].has_directory_create());
}

// Test DirectorySubscribe message handling
TEST_F(ServerTest, DirectorySubscribeTest) {
    // Prepare server response
    synxpo::ServerMessage response;
    response.mutable_ok_subscribed()->set_directory_id("sub-dir-456");
    service_->PrepareResponse(response);
    
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send DirectorySubscribe request
    synxpo::ClientMessage request;
    request.set_request_id("sub-req-456");
    auto* subscribe = request.mutable_directory_subscribe();
    subscribe->set_directory_id("sub-dir-456");
    
    EXPECT_TRUE(stream->Write(request));
    
    // Read response
    synxpo::ServerMessage server_response;
    EXPECT_TRUE(stream->Read(&server_response));
    
    EXPECT_TRUE(server_response.has_ok_subscribed());
    EXPECT_EQ(server_response.ok_subscribed().directory_id(), "sub-dir-456");
    
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
    
    // Verify received message
    const auto& received = service_->GetReceivedMessages();
    ASSERT_EQ(received.size(), 1);
    EXPECT_EQ(received[0].request_id(), "sub-req-456");
    EXPECT_TRUE(received[0].has_directory_subscribe());
    EXPECT_EQ(received[0].directory_subscribe().directory_id(), "sub-dir-456");
}

// Test DirectoryUnsubscribe message handling
TEST_F(ServerTest, DirectoryUnsubscribeTest) {
    synxpo::ServerMessage response;
    response.mutable_ok_unsubscribed()->set_directory_id("unsub-dir-789");
    service_->PrepareResponse(response);
    
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    synxpo::ClientMessage request;
    request.set_request_id("unsub-req-789");
    auto* unsubscribe = request.mutable_directory_unsubscribe();
    unsubscribe->set_directory_id("unsub-dir-789");
    
    EXPECT_TRUE(stream->Write(request));
    
    synxpo::ServerMessage server_response;
    EXPECT_TRUE(stream->Read(&server_response));
    
    EXPECT_TRUE(server_response.has_ok_unsubscribed());
    EXPECT_EQ(server_response.ok_unsubscribed().directory_id(), "unsub-dir-789");
    
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
}

// Test RequestVersion message handling
TEST_F(ServerTest, RequestVersionTest) {
    // Prepare CheckVersion response
    synxpo::ServerMessage response;
    auto* check = response.mutable_check_version();
    
    auto* file1 = check->add_files();
    file1->set_id("version-file-1");
    file1->set_directory_id("version-dir-1");
    file1->set_version(5);
    file1->set_content_changed_version(3);
    file1->set_type(synxpo::FILE);
    file1->set_current_path("version_test.txt");
    file1->set_deleted(false);
    
    service_->PrepareResponse(response);
    
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send RequestVersion
    synxpo::ClientMessage request;
    request.set_request_id("version-req-1");
    auto* req_version = request.mutable_request_version();
    auto* dir_request = req_version->add_requests();
    dir_request->set_directory_id("version-dir-1");
    
    EXPECT_TRUE(stream->Write(request));
    
    // Read CheckVersion response
    synxpo::ServerMessage server_response;
    EXPECT_TRUE(stream->Read(&server_response));
    
    EXPECT_TRUE(server_response.has_check_version());
    EXPECT_EQ(server_response.check_version().files_size(), 1);
    
    const auto& file = server_response.check_version().files(0);
    EXPECT_EQ(file.id(), "version-file-1");
    EXPECT_EQ(file.directory_id(), "version-dir-1");
    EXPECT_EQ(file.version(), 5);
    EXPECT_EQ(file.content_changed_version(), 3);
    EXPECT_EQ(file.type(), synxpo::FILE);
    EXPECT_EQ(file.current_path(), "version_test.txt");
    EXPECT_FALSE(file.deleted());
    
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
}

// Test AskVersionIncrease message handling
TEST_F(ServerTest, AskVersionIncreaseTest) {
    // Prepare VersionIncreaseAllow response
    synxpo::ServerMessage response;
    response.mutable_version_increase_allow();
    service_->PrepareResponse(response);
    
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send AskVersionIncrease
    synxpo::ClientMessage request;
    request.set_request_id("increase-req-1");
    auto* ask_increase = request.mutable_ask_version_increase();
    
    auto* file_info = ask_increase->add_files();
    file_info->set_id("increase-file-1");
    file_info->set_directory_id("increase-dir-1");
    file_info->set_current_path("increase_test.txt");
    file_info->set_deleted(false);
    file_info->set_content_changed(true);
    file_info->set_type(synxpo::FILE);
    
    EXPECT_TRUE(stream->Write(request));
    
    // Read response
    synxpo::ServerMessage server_response;
    EXPECT_TRUE(stream->Read(&server_response));
    
    EXPECT_TRUE(server_response.has_version_increase_allow());
    
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
    
    // Verify the server received our AskVersionIncrease
    const auto& received = service_->GetReceivedMessages();
    ASSERT_EQ(received.size(), 1);
    EXPECT_TRUE(received[0].has_ask_version_increase());
    EXPECT_EQ(received[0].ask_version_increase().files_size(), 1);
    
    const auto& received_file = received[0].ask_version_increase().files(0);
    EXPECT_EQ(received_file.id(), "increase-file-1");
    EXPECT_EQ(received_file.directory_id(), "increase-dir-1");
    EXPECT_EQ(received_file.current_path(), "increase_test.txt");
    EXPECT_FALSE(received_file.deleted());
    EXPECT_TRUE(received_file.content_changed());
    EXPECT_EQ(received_file.type(), synxpo::FILE);
}

// Test multiple messages in single stream
TEST_F(ServerTest, MultipleMessagesTest) {
    // Prepare multiple responses
    synxpo::ServerMessage response1;
    response1.mutable_ok_directory_created()->set_directory_id("multi-dir-1");
    
    synxpo::ServerMessage response2;
    response2.mutable_ok_subscribed()->set_directory_id("multi-dir-1");
    
    service_->PrepareResponse(response1);
    service_->PrepareResponse(response2);
    
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send first message
    synxpo::ClientMessage request1;
    request1.set_request_id("multi-req-1");
    request1.mutable_directory_create();
    EXPECT_TRUE(stream->Write(request1));
    
    // Read first response
    synxpo::ServerMessage server_response1;
    EXPECT_TRUE(stream->Read(&server_response1));
    EXPECT_TRUE(server_response1.has_ok_directory_created());
    
    // Send second message
    synxpo::ClientMessage request2;
    request2.set_request_id("multi-req-2");
    auto* subscribe = request2.mutable_directory_subscribe();
    subscribe->set_directory_id("multi-dir-1");
    EXPECT_TRUE(stream->Write(request2));
    
    // Read second response
    synxpo::ServerMessage server_response2;
    EXPECT_TRUE(stream->Read(&server_response2));
    EXPECT_TRUE(server_response2.has_ok_subscribed());
    
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
    
    // Verify server received both messages
    const auto& received = service_->GetReceivedMessages();
    EXPECT_EQ(received.size(), 2);
    EXPECT_TRUE(received[0].has_directory_create());
    EXPECT_TRUE(received[1].has_directory_subscribe());
}

// Test FileWrite message handling
TEST_F(ServerTest, FileWriteTest) {
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send FileWrite message
    synxpo::ClientMessage request;
    request.set_request_id("write-req-1");
    auto* write = request.mutable_file_write();
    auto* chunk = write->mutable_chunk();
    chunk->set_id("write-file-1");
    chunk->set_directory_id("write-dir-1");
    chunk->set_data("test file content");
    chunk->set_offset(0);
    
    EXPECT_TRUE(stream->Write(request));
    
    // Send FileWriteEnd
    synxpo::ClientMessage end_request;
    end_request.set_request_id("write-end-req-1");
    end_request.mutable_file_write_end();
    
    EXPECT_TRUE(stream->Write(end_request));
    
    EXPECT_TRUE(stream->WritesDone());
    auto status = stream->Finish();
    EXPECT_TRUE(status.ok());
    
    // Verify received messages
    const auto& received = service_->GetReceivedMessages();
    EXPECT_EQ(received.size(), 2);
    
    EXPECT_TRUE(received[0].has_file_write());
    const auto& write_chunk = received[0].file_write().chunk();
    EXPECT_EQ(write_chunk.id(), "write-file-1");
    EXPECT_EQ(write_chunk.directory_id(), "write-dir-1");
    EXPECT_EQ(write_chunk.data(), "test file content");
    EXPECT_EQ(write_chunk.offset(), 0);
    
    EXPECT_TRUE(received[1].has_file_write_end());
}

// Test error handling - connection interruption
TEST_F(ServerTest, ConnectionInterruptionTest) {
    ClientContext context;
    auto stream = stub_->Stream(&context);
    ASSERT_NE(stream, nullptr);
    
    // Send a message
    synxpo::ClientMessage request;
    request.set_request_id("interrupt-req");
    request.mutable_directory_create();
    EXPECT_TRUE(stream->Write(request));
    
    // Simulate connection interruption by canceling context
    context.TryCancel();
    
    // Try to send another message - should fail
    synxpo::ClientMessage request2;
    request2.mutable_directory_create();
    // This might succeed or fail depending on timing
    stream->Write(request2);
    
    stream->WritesDone();
    auto status = stream->Finish();
    // Status should indicate cancellation
    EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
}