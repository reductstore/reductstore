// Copyright 2022 Alexey Timin
#include "reduct/api/entry_api.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::ReadOne;
using reduct::api::EntryApi;
using reduct::core::Error;
using reduct::core::Time;
using reduct::storage::IStorage;

using us = std::chrono::microseconds;

TEST_CASE("EntryApi::Write should write data in chunks") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  SECTION("ok") {
    auto [resp, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10");
    REQUIRE(err == Error::kOk);

    auto [headers, len, input, output] = resp;
    REQUIRE(headers.empty());
    REQUIRE(len == 0);

    REQUIRE(input("12345", false) == Error::kOk);
    REQUIRE(input("67890", true) == Error::kOk);

    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
    REQUIRE(ReadOne(*entry, reduct::core::Time() + us(1000001)).result == "1234567890");
  }

  SECTION("bucket doesn't exist") {
    REQUIRE(EntryApi::Write(storage.get(), "XXX", "entry-1", "1000001", "10").error ==
            Error{
                .code = 404,
                .message = "Bucket 'XXX' is not found",
            });
  }

  SECTION("wrong ts") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "XXXX", "10").error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'ts' parameter: XXXX should unix times in microseconds",
            });
  }

  SECTION("empty entry") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "", "1000001", "10").error ==
            Error{
                .code = 422,
                .message = "An empty entry name is not allowed",
            });
  }

  SECTION("wrong content-length") {
    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();

    SECTION("empty") {
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "").error ==
              Error{
                  .code = 411,
                  .message = "bad or empty content-length",
              });
    }

    SECTION("negative") {
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "-1").error ==
              Error{
                  .code = 411,
                  .message = "negative content-length",
              });
    }

    SECTION("no number") {
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "xxxx").error ==
              Error{
                  .code = 411,
                  .message = "bad or empty content-length",
              });
    }
    REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error.code == 404);
  }

  SECTION("wrong input") {
    auto [resp, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10");
    REQUIRE(err == Error::kOk);

    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();

    SECTION("too long") {
      REQUIRE(resp.input_call("1234567890X", true) == Error{
                                                          .code = 413,
                                                          .message = "Content is bigger than in content-length",
                                                      });
      REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error == Error{
                                                                                .code = 500,
                                                                                .message = "Record is broken",
                                                                            });
    }

    SECTION("too short") {
      REQUIRE(resp.input_call("1234", true) == Error{
                                                   .code = 413,
                                                   .message = "Content is smaller than in content-length",
                                               });
      REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error == Error{
                                                                                .code = 500,
                                                                                .message = "Record is broken",
                                                                            });
    }
  }
}
