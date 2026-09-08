add_library(synxpo_db INTERFACE)

target_include_directories(synxpo_db
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(synxpo_db INTERFACE synxpo_domain)
target_compile_features(synxpo_db INTERFACE cxx_std_20)
