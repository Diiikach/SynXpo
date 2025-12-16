add_executable(test_sqlite_storage
    sqlite_file_storage_test.cpp
)

target_link_libraries(test_sqlite_storage
    PRIVATE
        synxpo_common
        GTest::gtest_main
        GTest::gmock
)

include(GoogleTest)
gtest_discover_tests(test_sqlite_storage)
