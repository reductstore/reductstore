enable_testing()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set (SRC_FILES
        test.cc reduct/storage/storage_test.cc)

add_executable(reduct_tests ${SRC_FILES})
target_link_libraries(reduct_tests reduct_lib ${CONAN_LIBS})