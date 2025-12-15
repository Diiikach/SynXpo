set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/synxpo.proto)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR})

set(GENERATED_SRCS
    ${GENERATED_DIR}/synxpo.pb.cc
    ${GENERATED_DIR}/synxpo.grpc.pb.cc
)

set(GENERATED_HDRS
    ${GENERATED_DIR}/synxpo.pb.h
    ${GENERATED_DIR}/synxpo.grpc.pb.h
)

if (TARGET protobuf::protoc)
    set(PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
    set(PROTOC_DEP protobuf::protoc)
elseif (TARGET protobuf::protoc_executable)
    set(PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc_executable>)
    set(PROTOC_DEP protobuf::protoc_executable)
else()
    message(FATAL_ERROR "protoc executable target not found (expected protobuf::protoc)")
endif()

add_custom_command(
    OUTPUT ${GENERATED_SRCS} ${GENERATED_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
        --cpp_out=${GENERATED_DIR}
        --grpc_out=${GENERATED_DIR}
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE} ${PROTOC_DEP} grpc_cpp_plugin
    COMMENT "Generating C++ sources from ${PROTO_FILE}"
    VERBATIM
)

add_library(synxpo_proto
    ${GENERATED_SRCS}
    ${GENERATED_HDRS}
)

target_link_libraries(synxpo_proto
    PUBLIC
        ${_GRPC_GRPCPP}
        ${_REFLECTION}
        ${_PROTOBUF_LIBPROTOBUF}
)

target_include_directories(synxpo_proto
    PUBLIC
        ${GENERATED_DIR}
        ${CMAKE_SOURCE_DIR}/include
)
