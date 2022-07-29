set(PROTO_SPEC_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# Generate protoc files
set(PROTOBUF_SPEC_FILES
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/auth.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/bucket.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/server.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/api/entry.proto
        ${PROTO_SPEC_ROOT_DIR}/reduct/proto/storage/entry.proto
        )

set(PROTOBUF_FILES
        ${CMAKE_BINARY_DIR}/reduct/proto/api/auth.pb.cc
        ${CMAKE_BINARY_DIR}/reduct/proto/api/bucket.pb.cc
        ${CMAKE_BINARY_DIR}/reduct/proto/api/server.pb.cc
        ${CMAKE_BINARY_DIR}/reduct/proto/api/entry.pb.cc
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
        reduct/asset/asset_manager.cc
        reduct/async/loop.cc
        reduct/auth/token_auth.cc

        reduct/core/env_variable.cc
        reduct/core/logger.cc
        reduct/core/error.cc

        reduct/storage/io/async_reader.cc
        reduct/storage/io/async_writer.cc
        reduct/storage/bucket.cc
        reduct/storage/entry.cc
        reduct/storage/storage.cc
        reduct/storage/block_manager.cc)


add_library(reduct STATIC ${SRC_FILES} ${PROTOBUF_FILES})


configure_file(config.h.in ${CMAKE_BINARY_DIR}/reduct/config.h @ONLY)

add_executable(reduct-storage main.cc)

if (WEB_CONSOLE_PATH)
    message(STATUS "Embedding Console from ${WEB_CONSOLE_PATH}")

    add_custom_command(OUTPUT console.o
            COMMAND cd ${WEB_CONSOLE_PATH} && zip ${CMAKE_CURRENT_BINARY_DIR}/console.zip -r ./
            COMMAND cd ${CMAKE_CURRENT_BINARY_DIR} && ld -r -b binary -o console.o console.zip
            COMMAND objcopy --rename-section .data=.rodata,alloc,load,readonly,data,contents
                ${CMAKE_CURRENT_BINARY_DIR}/console.o ${CMAKE_CURRENT_BINARY_DIR}/console.o)

    add_library(console STATIC console.o)

    set_source_files_properties(
            console.o
            PROPERTIES
            EXTERNAL_OBJECT true
            GENERATED true
    )

    set_target_properties(
            console
            PROPERTIES
            LINKER_LANGUAGE C
    )

    target_compile_definitions(reduct-storage PRIVATE WITH_CONSOLE)

    target_link_libraries(reduct-storage reduct ${CONAN_LIBS} console)
else ()
    target_link_libraries(reduct-storage reduct ${CONAN_LIBS})
endif ()


