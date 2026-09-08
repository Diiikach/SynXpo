cmake_minimum_required(VERSION 4.0)

project(SynXpo VERSION 2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SYNXPO_BUILD_TESTS "Build SynXpo tests" ON)
option(SYNXPO_BUILD_SERVER "Build the userver HTTP/gRPC server" OFF)

if(SYNXPO_BUILD_TESTS)
    enable_testing()
endif()

add_subdirectory(libs/domain)
add_subdirectory(libs/db)
add_subdirectory(libs/staging_fs)

if(SYNXPO_BUILD_SERVER)
    include(FetchContent)
    set(USERVER_FEATURE_GRPC ON CACHE BOOL "" FORCE)
    set(USERVER_FEATURE_POSTGRESQL ON CACHE BOOL "" FORCE)
    set(USERVER_FEATURE_TESTSUITE OFF CACHE BOOL "" FORCE)
    set(USERVER_FEATURE_UTEST OFF CACHE BOOL "" FORCE)
    set(USERVER_INSTALL OFF CACHE BOOL "" FORCE)
    # Do not make the service depend on Homebrew-only static archives on macOS.
    set(USERVER_USE_STATIC_LIBS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(userver
        GIT_REPOSITORY https://github.com/userver-framework/userver.git
        GIT_TAG v3.1
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(userver)
    add_subdirectory(apps/server)
endif()
