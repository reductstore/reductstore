set(SRC_FILES
        reduct/core/logger.cc
        )


add_library(reduct_lib STATIC ${SRC_FILES})


add_executable(reduct_storage main.cc)
target_link_libraries(reduct_storage reduct_lib ${CONAN_LIBS})
