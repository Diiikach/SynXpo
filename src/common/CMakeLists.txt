add_library(synxpo_common STATIC
    in_memory_file_storage.cpp
)

target_include_directories(synxpo_common
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(synxpo_common
    PUBLIC
        synxpo_proto
        absl::status
        absl::statusor
)
