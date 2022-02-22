set(PROTO_SPEC_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# Generate protoc files
set(PROTOBUF_SPEC_FILES
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/auth.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/bucket.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/server.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/storage/entry.proto
        )

set(PROTOBUF_FILES
        ${CMAKE_BINARY_DIR}/reduct/proto/api/auth.pb.cc
        ${CMAKE_BINARY_DIR}/reduct/proto/api/bucket.pb.cc
        ${CMAKE_BINARY_DIR}/reduct/proto/api/server.pb.cc
        ${CMAKE_BINARY_DIR}/reduct/proto/storage/entry.pb.cc

        )


add_custom_command(OUTPUT ${PROTOBUF_FILES}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I=${PROTO_SPEC_ROOT_DIR}/
        --cpp_out=${CMAKE_BINARY_DIR} ${PROTO_SPEC_ROOT_DIR}/reduct/proto/storage/*

        COMMAND ${Protobuf_PROTOC_EXECUTABLE} -I=${PROTO_SPEC_ROOT_DIR}/
        --cpp_out=${CMAKE_BINARY_DIR} ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/*

        DEPENDS ${PROTOBUF_SPEC_FILES}
        )


set(SRC_FILES
        reduct/api/api_server.cc
        reduct/async/loop.cc
        reduct/auth/token_auth.cc

        reduct/core/env_variable.cc
        reduct/core/logger.cc
        reduct/core/error.cc

        reduct/storage/bucket.cc
        reduct/storage/entry.cc
        reduct/storage/storage.cc)


add_library(reduct STATIC ${SRC_FILES} ${PROTOBUF_FILES})


configure_file(config.h.in ${CMAKE_BINARY_DIR}/reduct/config.h @ONLY)

add_executable(reduct-storage main.cc)
target_link_libraries(reduct-storage reduct ${CONAN_LIBS})
