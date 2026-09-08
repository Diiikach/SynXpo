cmake_minimum_required(VERSION 4.0)

project(SynXpo VERSION 2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SYNXPO_BUILD_TESTS "Build SynXpo tests" ON)

find_package(SQLite3 REQUIRED)

file(GLOB SYNXPO_SQLITE_MIGRATIONS CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/migrations/sqlite/*.sql"
)
file(COPY "${CMAKE_SOURCE_DIR}/migrations/sqlite/"
    DESTINATION "${CMAKE_BINARY_DIR}/migrations/sqlite"
)

if(SYNXPO_BUILD_TESTS)
    enable_testing()
endif()

add_subdirectory(libs/domain)
add_subdirectory(libs/db)
add_subdirectory(libs/db_sqlite)
