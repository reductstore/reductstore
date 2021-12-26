set(SRC_FILES
        reduct/core/logger.cc
        )


add_library(reduct_lib STATIC ${SRC_FILES})


configure_file(config.h.in ${CMAKE_BINARY_DIR}/reduct/config.h @ONLY)


add_executable(reduct_storage main.cc config.h.in)
target_link_libraries(reduct_storage reduct_lib ${CONAN_LIBS})
