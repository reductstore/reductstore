if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build --verbose)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo build --release --verbose)
    set(TARGET_DIR "release")
endif ()


set(RUST_PART_LIB "${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/librust_part.a")
set(RUST_PART_CXX "${CMAKE_CURRENT_BINARY_DIR}/rust_part.cpp")
set(RUST_PART_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/build.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/lib.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/asset.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/asset/asset_manager.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/auth.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/auth/policy.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/auth/proto.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/auth/token_auth.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/auth/token_repository.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/core.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/core/env.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/core/logger.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/auth.proto"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/storage.proto"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/block_manager.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/bucket.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/entry.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/reader.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/writer.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/storage.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/proto.rs"
        )

add_library(rust_part STATIC ${RUST_PART_CXX})

set(RUST_BUILD_COMMAND PROTOC=${Protobuf_PROTOC_EXECUTABLE} CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} ${CARGO_CMD})
set(RUST_TEST_COMMAND PROTOC=${Protobuf_PROTOC_EXECUTABLE} CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} cargo test --verbose)

add_custom_command(
        OUTPUT ${RUST_PART_CXX}
        COMMAND ${RUST_BUILD_COMMAND}
        COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/rust_part/src/lib.rs.cc ${RUST_PART_CXX}
        COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/rust_part/src/lib.rs.h ${CMAKE_CURRENT_BINARY_DIR}/rust_part.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS ${RUST_PART_SOURCES}
)

target_link_libraries(rust_part pthread dl ${RUST_PART_LIB})
target_include_directories(rust_part PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

add_test(NAME rust_part_test
        COMMAND cargo src/tests
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
