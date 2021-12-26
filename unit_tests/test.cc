// Copyright 2021 Alexey Timin
#define GTEST_DONT_DEFINE_FAIL 1
#define GTEST_DONT_DEFINE_SUCCEED 1
#define CATCH_CONFIG_RUNNER

#include <catch2/catch.hpp>
#include <gmock/gmock.h>

#include <string>

/**
 * Main function for Catch2 tests to work with GMock
 * https://github.com/matepek/catch2-with-gmock
 * @param argc
 * @param argv
 * @return
 */

int main(int argc, char** argv) {
  int gmockArgC = 1;
  ::testing::InitGoogleMock(&gmockArgC, argv);

  struct Listener : public testing::EmptyTestEventListener {
    void OnTestPartResult(const testing::TestPartResult& result) override {
      std::string filename = "unknown";
      size_t linenumber = 0;
      std::string message = "unknown";

      if (result.file_name() != nullptr) filename = result.file_name();

      if (result.line_number() != -1) {
        linenumber = static_cast<std::size_t>(result.line_number());
      }

      if (result.message() != nullptr) message = result.message();

      ::Catch::SourceLineInfo sourceLineInfo(filename.c_str(), linenumber);

      if (result.fatally_failed()) {
        ::Catch::AssertionHandler assertion("GTEST", sourceLineInfo, "",
                                            ::Catch::ResultDisposition::Normal);

        assertion.handleMessage(::Catch::ResultWas::ExplicitFailure, message);

        assertion.setCompleted();
      } else if (result.nonfatally_failed()) {
        ::Catch::AssertionHandler assertion(
            "GTEST", sourceLineInfo, "",
            ::Catch::ResultDisposition::ContinueOnFailure);

        assertion.handleMessage(::Catch::ResultWas::ExplicitFailure, message);

        assertion.setCompleted();
      }
    }
  };

  ::testing::UnitTest::GetInstance()->listeners().Append(new Listener);

  delete ::testing::UnitTest::GetInstance()->listeners().Release(
      ::testing::UnitTest::GetInstance()->listeners().default_result_printer());

  Catch::Session session;

  int returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    // Indicates a command line error
    return returnCode;
  }

  int result = session.run();

  return result;
}
