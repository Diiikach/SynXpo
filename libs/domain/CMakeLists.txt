add_library(synxpo_domain STATIC src/upload_session.cpp)

target_include_directories(synxpo_domain
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(synxpo_domain PUBLIC cxx_std_20)

if(SYNXPO_BUILD_TESTS)
    add_executable(upload_session_test tests/upload_session_test.cpp)
    target_link_libraries(upload_session_test PRIVATE synxpo_domain)
    add_test(NAME upload_session_test COMMAND upload_session_test)
endif()
