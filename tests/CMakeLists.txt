# Find Google Test
find_package(GTest)

if(NOT GTest_FOUND)
    # If not found, fetch it
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.14.0
    )
    FetchContent_MakeAvailable(googletest)
endif()

# Create test executable for server
add_executable(test_server
    test_server.cpp
)

target_link_libraries(test_server
    PRIVATE
    synxpo_proto
    synxpo_common
    ${_PROTOBUF_LIBPROTOBUF}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    Threads::Threads
    gtest
    gtest_main
)

target_include_directories(test_server
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src/server
)

# Add tests
add_test(NAME server_tests COMMAND test_server)

# Create test executable for protobuf messages
add_executable(test_protobuf
    test_protobuf.cpp
)

target_link_libraries(test_protobuf
    PRIVATE
    synxpo_proto
    ${_PROTOBUF_LIBPROTOBUF}
    gtest
    gtest_main
)

add_test(NAME protobuf_tests COMMAND test_protobuf)

# Create test executable for client
add_executable(test_client
    test_client.cpp
)

# Find and link absl
find_package(absl REQUIRED)

# Check if fswatch is available for tests too
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(FSWATCH_TEST libfswatch)
endif()

target_link_libraries(test_client
    PRIVATE
    synxpo_client
    ${_PROTOBUF_LIBPROTOBUF}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    gtest
    gtest_main
)

if(FSWATCH_TEST_FOUND)
    target_link_libraries(test_client PRIVATE ${FSWATCH_TEST_LIBRARIES})
    target_link_directories(test_client PRIVATE ${FSWATCH_TEST_LIBRARY_DIRS})
endif()

target_include_directories(test_client
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src/client
)

add_test(NAME client_tests COMMAND test_client)

# Create integration test executable
add_executable(test_integration
    test_integration.cpp
)

target_link_libraries(test_integration
    PRIVATE
    synxpo_proto
    synxpo_server_lib
    ${_PROTOBUF_LIBPROTOBUF}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    Threads::Threads
    gtest
    gtest_main
)

target_include_directories(test_integration
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

add_test(NAME integration_tests COMMAND test_integration)