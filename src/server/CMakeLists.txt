add_executable(synxpo-server
    server_main.cpp
)

target_link_libraries(synxpo-server
    PRIVATE
        synxpo_proto
        Threads::Threads
)

target_include_directories(synxpo-server
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

install(TARGETS synxpo-server 
    RUNTIME DESTINATION bin
    COMPONENT server
)
