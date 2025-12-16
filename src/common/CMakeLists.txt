add_library(synxpo_common STATIC
    in_memory_file_storage.cpp
    sqlite_file_storage.cpp
)

# Find SQLite3
find_package(SQLite3 REQUIRED)

target_include_directories(synxpo_common
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(synxpo_common
    PUBLIC
        synxpo_proto
        absl::status
        absl::statusor
        SQLite::SQLite3
)
