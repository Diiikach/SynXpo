set(CLIENT_SOURCES
        main.cpp
        file_watcher.cpp
        grpc_client.cpp
)

# Check if fswatch is available
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(FSWATCH libfswatch)
endif()

if(FSWATCH_FOUND)
    # Use cross-platform fswatch implementation
    list(APPEND CLIENT_SOURCES file_watcher_fswatch.cpp)
    message(STATUS "Using fswatch for file watching")
elseif(UNIX AND NOT APPLE)
    # Fallback to Linux-specific inotify implementation
    list(APPEND CLIENT_SOURCES file_watcher_linux.cpp)
    message(STATUS "Using inotify for file watching")
else()
    message(FATAL_ERROR "Unsupported platform for FileWatcher. Please install fswatch library.")
endif()

add_executable(synxpo-client ${CLIENT_SOURCES})

target_link_libraries(synxpo-client
        PRIVATE
        synxpo_proto
        Threads::Threads
)

if(FSWATCH_FOUND)
    target_include_directories(synxpo-client PRIVATE ${FSWATCH_INCLUDE_DIRS})
    target_link_libraries(synxpo-client PRIVATE ${FSWATCH_LIBRARIES})
    target_link_directories(synxpo-client PRIVATE ${FSWATCH_LIBRARY_DIRS})
endif()

target_include_directories(synxpo-client
        PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

install(TARGETS synxpo-client
        RUNTIME DESTINATION bin
        COMPONENT client
)