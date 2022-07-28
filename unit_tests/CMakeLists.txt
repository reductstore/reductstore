enable_testing()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(SRC_FILES
        reduct/asset/asset_manager_test.cc

        reduct/async/run_test.cc
        reduct/async/sleep_test.cc
        reduct/async/task_test.cc

        reduct/core/env_test.cc
        reduct/auth/token_auth_test.cc

        reduct/storage/io/async_io_test.cc
        reduct/storage/bucket_test.cc
        reduct/storage/entry_test.cc
        reduct/storage/entry_query_test.cc
        reduct/storage/storage_bucket_api_test.cc
        reduct/storage/storage_server_api_test.cc
        reduct/storage/storage_entry_api_test.cc
        test.cc)

add_executable(reduct-tests ${SRC_FILES})
target_link_libraries(reduct-tests reduct ${CONAN_LIBS})