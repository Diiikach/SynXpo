add_executable(session_repository_test
    session_repository_test.cpp
)

target_link_libraries(session_repository_test
    PRIVATE
        synxpo_session_core
)

add_test(NAME session_repository_test COMMAND session_repository_test)
