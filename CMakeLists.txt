cmake_minimum_required(VERSION 4.0)
project(SynXpo VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# Automatically fetch and build gRPC and dependencies if not found
find_package(Threads REQUIRED)

# Try to find installed gRPC first
if(NOT FORCE_GRPC_DOWNLOAD)
    find_package(Protobuf CONFIG)
    find_package(gRPC CONFIG)
endif()

# If gRPC not found, fetch it automatically
if(NOT gRPC_FOUND OR FORCE_GRPC_DOWNLOAD)
    message(STATUS "gRPC not found, fetching from source...")
    
    FetchContent_Declare(
        gRPC
        GIT_REPOSITORY https://github.com/grpc/grpc
        GIT_TAG        v1.60.0
        GIT_SHALLOW    TRUE
        EXCLUDE_FROM_ALL
    )
    
    set(FETCHCONTENT_QUIET OFF)
    set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CSHARP_EXT OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
    
    # Fix for Abseil/Protobuf install issues - disable their installation
    set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
    set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
    
    FetchContent_MakeAvailable(gRPC)
    
    # Set variables for compatibility
    set(_PROTOBUF_LIBPROTOBUF libprotobuf)
    set(_REFLECTION grpc++_reflection)
    set(_GRPC_GRPCPP grpc++)
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
else()
    message(STATUS "Using installed gRPC")
    set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
    set(_REFLECTION gRPC::grpc++_reflection)
    set(_GRPC_GRPCPP gRPC::grpc++)
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Add subdirectories
add_subdirectory(src/proto)
add_subdirectory(src/common)
add_subdirectory(src/server)
add_subdirectory(src/client)

# Enable testing
option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
