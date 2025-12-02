add_executable(synxpo-client
    client_main.cpp
)

target_link_libraries(synxpo-client
    PRIVATE
        synxpo_proto
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