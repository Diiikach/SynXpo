cmake_minimum_required(VERSION 4.0)

project(SynXpo VERSION 2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SYNXPO_BUILD_TESTS "Build SynXpo tests" ON)

find_package(SQLite3 REQUIRED)

add_subdirectory(src/server)

if(SYNXPO_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
