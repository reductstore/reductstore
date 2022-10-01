// Copyright 2022 Alexey Timin
#include "reduct/api/entry_api.h"

#include <catch2/catch.hpp>
#include <fmt/core.h>

#include <thread>

#include "reduct/helpers.h"

using reduct::ReadOne;
using reduct::WriteOne;
using reduct::api::EntryApi;
using reduct::core::Error;
using reduct::core::Time;
using reduct::proto::api::QueryInfo;
using reduct::storage::IStorage;

using google::protobuf::util::JsonStringToMessage;

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

TEST_CASE("EntryApi::Read should read data in chunks with time") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
  REQUIRE(WriteOne(*entry, "1234567890", Time() + us(1000001)) == Error::kOk);

  SECTION("ok") {
    auto [resp, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", "1000001", {});
    REQUIRE(err == Error::kOk);

    auto [headers, len, input, output] = resp;
    REQUIRE(headers["content-type"] == "application/octet-stream");
    REQUIRE(headers["x-reduct-time"] == "1000001");
    REQUIRE(headers["x-reduct-last"] == "1");
    REQUIRE(len == 10);
    REQUIRE(input("", true) == Error::kOk);

    auto ret = output();
    REQUIRE(ret == Error::kOk);
    REQUIRE(ret.result == "1234567890");
  }

  SECTION("bucket doesn't exist") {
    REQUIRE(EntryApi::Read(storage.get(), "XXX", "entry-1", "1000001", {}).error ==
            Error{
                .code = 404,
                .message = "Bucket 'XXX' is not found",
            });
  }

  SECTION("entry doesn't exist") {
    REQUIRE(EntryApi::Read(storage.get(), "bucket", "XXX", "1000001", {}).error ==
            Error{
                .code = 404,
                .message = "Entry 'XXX' is not found",
            });
  }

  SECTION("record doesn't exist") {
    REQUIRE(EntryApi::Read(storage.get(), "bucket", "entry-1", "1234567", {}).error ==
            Error{
                .code = 404,
                .message = "No records for this timestamp",
            });
  }

  SECTION("wrong ts") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "XXXX", {}).error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'ts' parameter: XXXX should unix times in microseconds",
            });
  }
}

TEST_CASE("EntryApi::Read should read data in chunks with query id") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
  REQUIRE(WriteOne(*entry, "1234567890", Time() + us(1000001)) == Error::kOk);
  REQUIRE(WriteOne(*entry, "abcd", Time() + us(2000001)) == Error::kOk);

  SECTION("ok") {
    auto id = std::to_string(entry->Query({}, {}, {}).result);
    {
      auto [resp, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id);
      REQUIRE(err == Error::kOk);

      auto [headers, len, input, output] = resp;
      REQUIRE(headers["content-type"] == "application/octet-stream");
      REQUIRE(headers["x-reduct-time"] == "1000001");
      REQUIRE(headers["x-reduct-last"] == "0");
      REQUIRE(len == 10);
      REQUIRE(input("", true) == Error::kOk);

      auto ret = output();
      REQUIRE(ret == Error::kOk);
      REQUIRE(ret.result == "1234567890");
    }
    {
      auto [resp, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id);
      REQUIRE(err == Error::kOk);

      auto [headers, len, input, output] = resp;
      REQUIRE(headers["content-type"] == "application/octet-stream");
      REQUIRE(headers["x-reduct-time"] == "2000001");
      REQUIRE(headers["x-reduct-last"] == "1");
      REQUIRE(len == 4);
      REQUIRE(input("", true) == Error::kOk);

      auto ret = output();
      REQUIRE(ret == Error::kOk);
      REQUIRE(ret.result == "abcd");
    }

    {
      auto [resp, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id);
      REQUIRE(err ==
              Error{.code = 404, .message = fmt::format("Query id={} doesn't exist. It expired or was finished", id)});
    }
  }

  SECTION("id doesnt' exist") {
    auto [resp, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, "2");
    REQUIRE(err == Error{.code = 404, .message = "Query id=2 doesn't exist. It expired or was finished"});
  }

  SECTION("wrong id") {
    auto [resp, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, "XXX2");
    REQUIRE(err == Error{.code = 422, .message = "Failed to parse 'id' parameter: XXX2 should be unsigned integer"});
  }
}

TEST_CASE("EntryApi::Query should query data for time interval") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
  REQUIRE(WriteOne(*entry, "1234567890", Time() + us(1000001)) == Error::kOk);
  REQUIRE(WriteOne(*entry, "abcd", Time() + us(2000001)) == Error::kOk);

  QueryInfo info;
  SECTION("ok all") {
    auto [resp, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, {});
    REQUIRE(err == Error::kOk);

    auto [out, out_err] = resp.output_call();
    REQUIRE(err == Error::kOk);
    JsonStringToMessage(out, &info);

    REQUIRE(entry->Next(info.id()).error == Error::kOk);
    REQUIRE(entry->Next(info.id()).error == Error::kOk);
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok start") {
    auto [resp, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000002", {}, {});
    REQUIRE(err == Error::kOk);

    JsonStringToMessage(resp.output_call().result, &info);

    REQUIRE(entry->Next(info.id()).result.reader->timestamp() == Time() + us(2000001));
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok stop") {
    auto [resp, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, "1000002", {});
    REQUIRE(err == Error::kOk);

    JsonStringToMessage(resp.output_call().result, &info);

    REQUIRE(entry->Next(info.id()).result.reader->timestamp() == Time() + us(1000001));
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok timeinterval") {
    auto [resp, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000003", "1000004", {});
    REQUIRE(err == Error::kOk);

    JsonStringToMessage(resp.output_call().result, &info);

    REQUIRE(entry->Next(info.id()).error.code == 202);  // no content
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok ttl") {
    auto [resp, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000003", "1000004", "1");
    REQUIRE(err == Error::kOk);

    std::this_thread::sleep_for(us(1'000'000));
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("bucket doesn't exist") {
    REQUIRE(EntryApi::Query(storage.get(), "XXX", "entry-1", {}, {}, {}).error ==
            Error{
                .code = 404,
                .message = "Bucket 'XXX' is not found",
            });
  }

  SECTION("entry doesn't exist") {
    REQUIRE(EntryApi::Query(storage.get(), "bucket", "XXX", {}, {}, {}).error ==
            Error{
                .code = 404,
                .message = "Entry 'XXX' is not found",
            });
  }

  SECTION("wrong parameters") {
    REQUIRE(EntryApi::Query(storage.get(), "bucket", "entry-1", "XXX", {}, {}).error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'start_timestamp' parameter: XXX should unix times in microseconds",
            });

    REQUIRE(EntryApi::Query(storage.get(), "bucket", "entry-1", {}, "XXX", {}).error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'stop_timestamp' parameter: XXX should unix times in microseconds",
            });

    REQUIRE(EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, "XXX").error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'ttl' parameter: XXX should be unsigned integer",
            });
  }
}
