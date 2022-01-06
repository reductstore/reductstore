enable_testing()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set (SRC_FILES
        reduct/async/run_test.cc
        reduct/async/sleep_test.cc
        reduct/async/task_test.cc

        reduct/storage/bucket_test.cc
        reduct/storage/entry_test.cc
        reduct/storage/storage_test.cc
        test.cc)

add_executable(reduct-tests ${SRC_FILES})
target_link_libraries(reduct-tests reduct ${CONAN_LIBS})