add_library(synxpo_session_core STATIC
    upload_session.cpp
    sqlite_session_repository.cpp
)

target_include_directories(synxpo_session_core
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(synxpo_session_core
    PUBLIC
        SQLite::SQLite3
)

target_compile_features(synxpo_session_core PUBLIC cxx_std_20)
