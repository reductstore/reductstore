cmake_minimum_required(VERSION 3.18)

set(MAJOR_VERSION 0)
set(MINOR_VERSION 7)
set(PATCH_VERSION 1)

set(FULL_VERSION ${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION})

option(REDUCT_BUILD_TEST "Build unit tests" ON)
option(REDUCT_BUILD_BENCHMARKS "Build unit tests" ON)


set(DEFAULT_MAX_BLOCK_SIZE 64000000 CACHE STRING "Default max size for block with data")
set(DEFAULT_MAX_BLOCK_RECORDS 1024 CACHE STRING "Default max number of records in a block")
set(DEFAULT_MAX_READ_CHUNK 512000 CACHE STRING "Default max chunk for reading")
set(WEB_CONSOLE_PATH "" CACHE STRING "Path to the built web console")

project(reduct_storage VERSION ${FULL_VERSION})

message(STATUS "Version ${FULL_VERSION}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

if (FULL_STATIC_BINARY)
    set(CMAKE_EXE_LINKER_FLAGS " -static")
endif ()

if (NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/v0.16.1/conan.cmake"
            "${CMAKE_BINARY_DIR}/conan.cmake"
            EXPECTED_HASH SHA256=396e16d0f5eabdc6a14afddbcfff62a54a7ee75c6da23f32f7a31bc85db23484
            TLS_VERIFY ON)
endif ()

include(${CMAKE_BINARY_DIR}/conan.cmake)
conan_cmake_autodetect(settings)
conan_cmake_install(PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}/conanfile.txt
        BUILD missing
        UPDATE OFF
        SETTINGS ${settings})

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()


include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${CMAKE_BINARY_DIR})

message(STATUS "Protobuf exe  ${Protobuf_PROTOC_EXECUTABLE}")

add_subdirectory(src)

message(STATUS "REDUCT_BUILD_TEST=${REDUCT_BUILD_TEST}")
if (REDUCT_BUILD_TEST)
    add_subdirectory(unit_tests)
endif ()

message(STATUS "REDUCT_BUILD_BENCHMARKS=${REDUCT_BUILD_BENCHMARKS}")
if (REDUCT_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif ()
