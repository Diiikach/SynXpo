# Create a library for generated proto files
add_library(synxpo_proto
    synxpo.proto
)

include(${protobuf_SOURCE_DIR}/cmake/protobuf-generate.cmake)

target_link_libraries(synxpo_proto
    PUBLIC
        ${_GRPC_GRPCPP}
        ${_REFLECTION}
        ${_PROTOBUF_LIBPROTOBUF}
)

target_include_directories(synxpo_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_SOURCE_DIR}/include
)

# Generate protobuf and gRPC files
protobuf_generate(
    TARGET synxpo_proto
    LANGUAGE cpp
    PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
)

protobuf_generate(
    TARGET synxpo_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}"
    PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
)
