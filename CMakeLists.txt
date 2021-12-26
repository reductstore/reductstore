cmake_minimum_required(VERSION 3.18)

project(reduct_storage VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)

if (FULL_STATIC_BINARY)
    set(CMAKE_EXE_LINKER_FLAGS " -static")
endif()

if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/v0.16.1/conan.cmake"
            "${CMAKE_BINARY_DIR}/conan.cmake"
            EXPECTED_HASH SHA256=396e16d0f5eabdc6a14afddbcfff62a54a7ee75c6da23f32f7a31bc85db23484
            TLS_VERIFY ON)
endif()

include(${CMAKE_BINARY_DIR}/conan.cmake)
conan_cmake_autodetect(settings)
conan_cmake_install(PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}/conanfile.txt
        BUILD missing
        UPDATE ON
        SETTINGS ${settings}
        PROFILE ${CONAN_PROFILE})

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

add_subdirectory(src)
