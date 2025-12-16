# Server library (for testing)
add_library(synxpo_server_lib STATIC
    uuid.cpp
    storage.cpp
    subscriptions.cpp
    service.cpp
)

target_link_libraries(synxpo_server_lib
    PUBLIC
        synxpo_proto
        Threads::Threads
)

target_include_directories(synxpo_server_lib
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

# Server executable
add_executable(synxpo-server
    main.cpp
)

target_link_libraries(synxpo-server
    PRIVATE
        synxpo_server_lib
)

install(TARGETS synxpo-server 
    RUNTIME DESTINATION bin
    COMPONENT server
)
