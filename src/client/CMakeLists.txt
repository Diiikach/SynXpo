set(CLIENT_SOURCES
        main.cpp
        file_watcher/file_watcher.cpp
        grpc_client.cpp
        config.cpp
        synchronizer/synchronizer.cpp
        synchronizer/synchronizer_init.cpp
        synchronizer/synchronizer_upload.cpp
        synchronizer/synchronizer_download.cpp
        synchronizer/synchronizer_handlers.cpp
)


# Check if fswatch is available
find_package(PkgConfig)

if (PkgConfig_FOUND)
    pkg_check_modules(FSWATCH libfswatch)
endif ()

if (FSWATCH_FOUND)
    list(APPEND CLIENT_SOURCES file_watcher/file_watcher_fswatch.cpp
    )
    message(STATUS "Using fswatch for file watching")
elseif (UNIX AND NOT APPLE)
    # Fallback to Linux-specific inotify implementation
    list(APPEND CLIENT_SOURCES file_watcher/file_watcher_linux.cpp
    )
    message(STATUS "Using inotify for file watching")
else ()
    message(FATAL_ERROR "Unsupported platform for FileWatcher. Please install fswatch library.")
endif ()

add_executable(synxpo-client ${CLIENT_SOURCES})

target_link_libraries(synxpo-client
        PRIVATE
        synxpo_proto
        synxpo_common
        absl::flags
        absl::flags_parse
        Threads::Threads
)

if (FSWATCH_FOUND)
    target_include_directories(synxpo-client PRIVATE ${FSWATCH_INCLUDE_DIRS})
    target_link_libraries(synxpo-client PRIVATE ${FSWATCH_LIBRARIES})
    target_link_directories(synxpo-client PRIVATE ${FSWATCH_LIBRARY_DIRS})
endif ()

target_include_directories(synxpo-client
        PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

install(TARGETS synxpo-client
        RUNTIME DESTINATION bin
        COMPONENT client
)