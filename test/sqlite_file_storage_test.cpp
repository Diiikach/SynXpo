#include "synxpo/common/sqlite_file_storage.h"

#include <filesystem>
#include <fstream>
#include <thread>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace synxpo {
namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class SqliteFileStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a unique temp directory for each test
        test_dir_ = std::filesystem::temp_directory_path() / 
                    ("synxpo_test_" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) +
                     "_" + std::to_string(test_counter_++));
        std::filesystem::create_directories(test_dir_);
        db_path_ = test_dir_ / "test.db";
    }

    void TearDown() override {
        storage_.reset();
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }

    FileMetadata CreateMetadata(const std::string& file_id,
                                 const std::string& directory_id,
                                 const std::string& path,
                                 uint64_t version = 1) {
        FileMetadata meta;
        meta.set_id(file_id);
        meta.set_directory_id(directory_id);
        meta.set_current_path(path);
        meta.set_version(version);
        meta.set_deleted(false);
        return meta;
    }

    std::filesystem::path test_dir_;
    std::filesystem::path db_path_;
    std::unique_ptr<SqliteFileMetadataStorage> storage_;
    static int test_counter_;
};

int SqliteFileStorageTest::test_counter_ = 0;

// ============================================================================
// Construction and Database Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, ConstructorCreatesDatabase) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    EXPECT_TRUE(std::filesystem::exists(db_path_));
}

TEST_F(SqliteFileStorageTest, ConstructorCreatesParentDirectories) {
    auto nested_path = test_dir_ / "a" / "b" / "c" / "test.db";
    storage_ = std::make_unique<SqliteFileMetadataStorage>(nested_path);
    EXPECT_TRUE(std::filesystem::exists(nested_path));
}

TEST_F(SqliteFileStorageTest, ReopenExistingDatabase) {
    {
        auto storage = std::make_unique<SqliteFileMetadataStorage>(db_path_);
        storage->RegisterDirectory("dir1", test_dir_ / "sync_folder");
    }
    
    // Reopen and verify data persists
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto dirs = storage_->ListDirectories();
    ASSERT_THAT(dirs, SizeIs(1));
    EXPECT_EQ(dirs[0], "dir1");
}

// ============================================================================
// Directory Management Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, RegisterDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto dirs = storage_->ListDirectories();
    ASSERT_THAT(dirs, SizeIs(1));
    EXPECT_EQ(dirs[0], "dir1");
}

TEST_F(SqliteFileStorageTest, RegisterMultipleDirectories) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto path1 = test_dir_ / "folder1";
    auto path2 = test_dir_ / "folder2";
    auto path3 = test_dir_ / "folder3";
    std::filesystem::create_directories(path1);
    std::filesystem::create_directories(path2);
    std::filesystem::create_directories(path3);
    
    storage_->RegisterDirectory("dir1", path1);
    storage_->RegisterDirectory("dir2", path2);
    storage_->RegisterDirectory("dir3", path3);
    
    auto dirs = storage_->ListDirectories();
    EXPECT_THAT(dirs, UnorderedElementsAre("dir1", "dir2", "dir3"));
}

TEST_F(SqliteFileStorageTest, RegisterDirectoryUpdatesPath) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto path1 = test_dir_ / "folder1";
    auto path2 = test_dir_ / "folder2";
    std::filesystem::create_directories(path1);
    std::filesystem::create_directories(path2);
    
    storage_->RegisterDirectory("dir1", path1);
    storage_->RegisterDirectory("dir1", path2);  // Update path
    
    auto dirs = storage_->ListDirectories();
    EXPECT_THAT(dirs, SizeIs(1));
}

TEST_F(SqliteFileStorageTest, UnregisterDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    
    storage_->RegisterDirectory("dir1", sync_path);
    storage_->UnregisterDirectory("dir1");
    
    auto dirs = storage_->ListDirectories();
    EXPECT_THAT(dirs, IsEmpty());
}

TEST_F(SqliteFileStorageTest, UnregisterDirectoryCascadesDeleteFiles) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto meta = CreateMetadata("file1", "dir1", "test.txt");
    ASSERT_TRUE(storage_->UpsertFile(meta).ok());
    
    storage_->UnregisterDirectory("dir1");
    
    // Re-register to check files are gone
    storage_->RegisterDirectory("dir1", sync_path);
    auto files = storage_->ListDirectoryFiles("dir1");
    ASSERT_TRUE(files.ok());
    EXPECT_THAT(*files, IsEmpty());
}

TEST_F(SqliteFileStorageTest, UnregisterNonexistentDirectoryIsNoop) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    // Should not throw or error
    storage_->UnregisterDirectory("nonexistent");
}

// ============================================================================
// GetDirectoryIdByPath Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, GetDirectoryIdByPathFindsDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto file_in_dir = sync_path / "subdir" / "file.txt";
    auto result = storage_->GetDirectoryIdByPath(file_in_dir);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "dir1");
}

TEST_F(SqliteFileStorageTest, GetDirectoryIdByPathReturnsNulloptForUnknownPath) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto outside_path = test_dir_ / "other_folder" / "file.txt";
    auto result = storage_->GetDirectoryIdByPath(outside_path);
    
    EXPECT_FALSE(result.has_value());
}

// ============================================================================
// File CRUD Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, UpsertFileAndGetById) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto meta = CreateMetadata("file1", "dir1", "test.txt", 5);
    meta.set_content_changed_version(3);
    
    auto status = storage_->UpsertFile(meta);
    ASSERT_TRUE(status.ok()) << status.message();
    
    auto retrieved = storage_->GetFileMetadata("dir1", std::string("file1"));
    ASSERT_TRUE(retrieved.ok()) << retrieved.status().message();
    
    EXPECT_EQ(retrieved->id(), "file1");
    EXPECT_EQ(retrieved->directory_id(), "dir1");
    EXPECT_EQ(retrieved->current_path(), "test.txt");
    EXPECT_EQ(retrieved->version(), 5);
    EXPECT_EQ(retrieved->content_changed_version(), 3);
}

TEST_F(SqliteFileStorageTest, UpsertFileAndGetByPath) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto meta = CreateMetadata("file1", "dir1", "subdir/test.txt");
    auto status = storage_->UpsertFile(meta);
    ASSERT_TRUE(status.ok());
    
    auto retrieved = storage_->GetFileMetadata("dir1", std::filesystem::path("subdir/test.txt"));
    ASSERT_TRUE(retrieved.ok()) << retrieved.status().message();
    
    EXPECT_EQ(retrieved->id(), "file1");
}

TEST_F(SqliteFileStorageTest, UpsertFileUpdatesExisting) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto meta1 = CreateMetadata("file1", "dir1", "test.txt", 1);
    ASSERT_TRUE(storage_->UpsertFile(meta1).ok());
    
    auto meta2 = CreateMetadata("file1", "dir1", "renamed.txt", 2);
    ASSERT_TRUE(storage_->UpsertFile(meta2).ok());
    
    auto retrieved = storage_->GetFileMetadata("dir1", std::string("file1"));
    ASSERT_TRUE(retrieved.ok());
    EXPECT_EQ(retrieved->current_path(), "renamed.txt");
    EXPECT_EQ(retrieved->version(), 2);
}

TEST_F(SqliteFileStorageTest, UpsertFileFailsWithoutDirectoryId) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    
    FileMetadata meta;
    meta.set_id("file1");
    meta.set_current_path("test.txt");
    // Missing directory_id
    
    auto status = storage_->UpsertFile(meta);
    EXPECT_FALSE(status.ok());
}

TEST_F(SqliteFileStorageTest, UpsertFileFailsWithoutFileId) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    FileMetadata meta;
    meta.set_directory_id("dir1");
    meta.set_current_path("test.txt");
    // Missing file id
    
    auto status = storage_->UpsertFile(meta);
    EXPECT_FALSE(status.ok());
}

TEST_F(SqliteFileStorageTest, UpsertFileFailsForUnregisteredDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    
    auto meta = CreateMetadata("file1", "nonexistent_dir", "test.txt");
    auto status = storage_->UpsertFile(meta);
    EXPECT_FALSE(status.ok());
}

TEST_F(SqliteFileStorageTest, GetFileMetadataNotFound) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto result = storage_->GetFileMetadata("dir1", std::string("nonexistent"));
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(absl::IsNotFound(result.status()));
}

TEST_F(SqliteFileStorageTest, GetFileMetadataByPathNotFound) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto result = storage_->GetFileMetadata("dir1", std::filesystem::path("nonexistent.txt"));
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(absl::IsNotFound(result.status()));
}

TEST_F(SqliteFileStorageTest, GetFileMetadataUnknownDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    
    auto result = storage_->GetFileMetadata("unknown", std::string("file1"));
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(absl::IsNotFound(result.status()));
}

// ============================================================================
// RemoveFile Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, RemoveFile) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto meta = CreateMetadata("file1", "dir1", "test.txt");
    ASSERT_TRUE(storage_->UpsertFile(meta).ok());
    
    auto status = storage_->RemoveFile("dir1", "file1");
    EXPECT_TRUE(status.ok());
    
    auto result = storage_->GetFileMetadata("dir1", std::string("file1"));
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(absl::IsNotFound(result.status()));
}

TEST_F(SqliteFileStorageTest, RemoveNonexistentFile) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto status = storage_->RemoveFile("dir1", "nonexistent");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(absl::IsNotFound(status));
}

TEST_F(SqliteFileStorageTest, RemoveFileUnknownDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    
    auto status = storage_->RemoveFile("unknown", "file1");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(absl::IsNotFound(status));
}

// ============================================================================
// ListDirectoryFiles Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, ListDirectoryFilesEmpty) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto result = storage_->ListDirectoryFiles("dir1");
    ASSERT_TRUE(result.ok());
    EXPECT_THAT(*result, IsEmpty());
}

TEST_F(SqliteFileStorageTest, ListDirectoryFilesMultiple) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    ASSERT_TRUE(storage_->UpsertFile(CreateMetadata("file1", "dir1", "a.txt")).ok());
    ASSERT_TRUE(storage_->UpsertFile(CreateMetadata("file2", "dir1", "b.txt")).ok());
    ASSERT_TRUE(storage_->UpsertFile(CreateMetadata("file3", "dir1", "c.txt")).ok());
    
    auto result = storage_->ListDirectoryFiles("dir1");
    ASSERT_TRUE(result.ok());
    EXPECT_THAT(*result, SizeIs(3));
    
    std::vector<std::string> ids;
    for (const auto& meta : *result) {
        ids.push_back(meta.id());
    }
    EXPECT_THAT(ids, UnorderedElementsAre("file1", "file2", "file3"));
}

TEST_F(SqliteFileStorageTest, ListDirectoryFilesOnlyFromSpecifiedDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto path1 = test_dir_ / "folder1";
    auto path2 = test_dir_ / "folder2";
    std::filesystem::create_directories(path1);
    std::filesystem::create_directories(path2);
    storage_->RegisterDirectory("dir1", path1);
    storage_->RegisterDirectory("dir2", path2);
    
    ASSERT_TRUE(storage_->UpsertFile(CreateMetadata("file1", "dir1", "a.txt")).ok());
    ASSERT_TRUE(storage_->UpsertFile(CreateMetadata("file2", "dir2", "b.txt")).ok());
    
    auto result1 = storage_->ListDirectoryFiles("dir1");
    ASSERT_TRUE(result1.ok());
    ASSERT_THAT(*result1, SizeIs(1));
    EXPECT_EQ((*result1)[0].id(), "file1");
    
    auto result2 = storage_->ListDirectoryFiles("dir2");
    ASSERT_TRUE(result2.ok());
    ASSERT_THAT(*result2, SizeIs(1));
    EXPECT_EQ((*result2)[0].id(), "file2");
}

TEST_F(SqliteFileStorageTest, ListDirectoryFilesUnknownDirectory) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    
    auto result = storage_->ListDirectoryFiles("unknown");
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(absl::IsNotFound(result.status()));
}

// ============================================================================
// Persistence Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, DataPersistsAcrossReopen) {
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    
    // Create storage, add data, close
    {
        auto storage = std::make_unique<SqliteFileMetadataStorage>(db_path_);
        storage->RegisterDirectory("dir1", sync_path);
        
        auto meta = CreateMetadata("file1", "dir1", "test.txt", 42);
        meta.set_content_changed_version(10);
        ASSERT_TRUE(storage->UpsertFile(meta).ok());
    }
    
    // Reopen and verify
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    
    auto dirs = storage_->ListDirectories();
    EXPECT_THAT(dirs, ElementsAre("dir1"));
    
    auto meta = storage_->GetFileMetadata("dir1", std::string("file1"));
    ASSERT_TRUE(meta.ok());
    EXPECT_EQ(meta->current_path(), "test.txt");
    EXPECT_EQ(meta->version(), 42);
    EXPECT_EQ(meta->content_changed_version(), 10);
}

// ============================================================================
// Deleted File Tests
// ============================================================================

TEST_F(SqliteFileStorageTest, UpsertDeletedFile) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    auto meta = CreateMetadata("file1", "dir1", "test.txt", 2);
    meta.set_deleted(true);
    
    ASSERT_TRUE(storage_->UpsertFile(meta).ok());
    
    auto retrieved = storage_->GetFileMetadata("dir1", std::string("file1"));
    ASSERT_TRUE(retrieved.ok());
    EXPECT_TRUE(retrieved->deleted());
}

// ============================================================================
// Thread Safety Tests (Basic)
// ============================================================================

TEST_F(SqliteFileStorageTest, ConcurrentReadsAndWrites) {
    storage_ = std::make_unique<SqliteFileMetadataStorage>(db_path_);
    auto sync_path = test_dir_ / "sync_folder";
    std::filesystem::create_directories(sync_path);
    storage_->RegisterDirectory("dir1", sync_path);
    
    constexpr int kNumThreads = 4;
    constexpr int kOpsPerThread = 50;
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([this, t]() {
            for (int i = 0; i < kOpsPerThread; ++i) {
                std::string file_id = "file_" + std::to_string(t) + "_" + std::to_string(i);
                auto meta = CreateMetadata(file_id, "dir1", "path_" + file_id + ".txt", i);
                
                EXPECT_TRUE(storage_->UpsertFile(meta).ok());
                
                auto retrieved = storage_->GetFileMetadata("dir1", std::string(file_id));
                EXPECT_TRUE(retrieved.ok());
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto files = storage_->ListDirectoryFiles("dir1");
    ASSERT_TRUE(files.ok());
    EXPECT_EQ(files->size(), kNumThreads * kOpsPerThread);
}

}  // namespace
}  // namespace synxpo
