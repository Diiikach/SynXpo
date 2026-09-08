add_library(synxpo_db_sqlite STATIC src/sqlite_upload_session_repository.cpp)

target_include_directories(synxpo_db_sqlite
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(synxpo_db_sqlite PUBLIC synxpo_db SQLite::SQLite3)
target_compile_features(synxpo_db_sqlite PUBLIC cxx_std_20)
target_compile_definitions(synxpo_db_sqlite
    PRIVATE SYNXPO_SQLITE_MIGRATIONS_DIRECTORY="${CMAKE_BINARY_DIR}/migrations/sqlite"
)

if(SYNXPO_BUILD_TESTS)
    add_executable(sqlite_upload_session_repository_test
        tests/sqlite_upload_session_repository_test.cpp
    )
    target_link_libraries(sqlite_upload_session_repository_test PRIVATE synxpo_db_sqlite)
    add_test(NAME sqlite_upload_session_repository_test COMMAND sqlite_upload_session_repository_test)
endif()
