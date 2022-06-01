// Copyright 2022 Alexey Timin
#define CATCH_CONFIG_RUNNER
#define CATCH_CONFIG_ENABLE_BENCHMARKING
#include <catch2/catch.hpp>

int main(int argc, char** argv) {
  Catch::Session session;

  int returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    // Indicates a command line error
    return returnCode;
  }

  int result = session.run();

  return result;
}
