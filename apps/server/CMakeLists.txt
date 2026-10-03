add_executable(synxpo-server main.cpp)

target_link_libraries(synxpo-server PRIVATE
    userver::core
    userver::grpc
    userver::postgresql
)
target_compile_features(synxpo-server PRIVATE cxx_std_20)

configure_file(config/config_vars.example.yaml config_vars.example.yaml COPYONLY)
