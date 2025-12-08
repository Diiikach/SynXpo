set(CLIENT_SOURCES
    main.cpp
    file_watcher.cpp
    grpc_client.cpp
    config.cpp
    synchronizer/synchronizer.cpp
    synchronizer/synchronizer_init.cpp
    synchronizer/synchronizer_upload.cpp
    synchronizer/synchronizer_download.cpp
    synchronizer/synchronizer_handlers.cpp
)

if(UNIX AND NOT APPLE)
    list(APPEND CLIENT_SOURCES file_watcher_linux.cpp)
else()
    message(FATAL_ERROR "Unsupported platform for FileWatcher")
endif()

add_executable(synxpo-client ${CLIENT_SOURCES})

target_link_libraries(synxpo-client
    PRIVATE
        synxpo_proto
        synxpo_common
        Threads::Threads
)

target_include_directories(synxpo-client
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

install(TARGETS synxpo-client
    RUNTIME DESTINATION bin
    COMPONENT client
)