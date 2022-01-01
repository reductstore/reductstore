set(SRC_FILES
        reduct/api/handlers/handle_bucket.cc
        reduct/api/handlers/handle_info.cc
        reduct/api/api_server.cc

        reduct/async/loop.cc

        reduct/core/env_variable.cc
        reduct/core/logger.cc
        reduct/core/error.cc

        reduct/storage/bucket.cc
        reduct/storage/storage.cc)


add_library(reduct STATIC ${SRC_FILES})


configure_file(config.h.in ${CMAKE_BINARY_DIR}/reduct/config.h @ONLY)


add_executable(reduct-storage main.cc config.h.in)
target_link_libraries(reduct-storage reduct ${CONAN_LIBS})
