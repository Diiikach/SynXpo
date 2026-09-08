add_library(synxpo_staging_fs INTERFACE)

target_include_directories(synxpo_staging_fs
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(synxpo_staging_fs INTERFACE cxx_std_20)
