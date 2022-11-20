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
    {
      auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10");
      REQUIRE(err == Error::kOk);

      REQUIRE(receiver("12345", false).error.code == 100);

      auto [resp, resp_err] = receiver("67890", true);
      REQUIRE(resp_err == Error::kOk);

      REQUIRE(resp.headers.empty());
      REQUIRE(resp.content_length == 0);

      auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
      REQUIRE(ReadOne(*entry, reduct::core::Time() + us(1000001)).result == "1234567890");
    }

    SECTION("record already exists") {
      auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10");
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error == Error{
                                              .code = 409,
                                              .message = "A record with timestamp 1000001 already exists",
                                          });
    }
  }

  SECTION("wrong ts") {
    auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "XXXX", "10");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'ts' parameter: XXXX must unix times in microseconds",
            });
  }

  SECTION("negative ts") {
    auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "-100", "10");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 422,
                                            .message = "Failed to parse 'ts' parameter: -100 must be positive",
                                        });
  }

  SECTION("empty entry") {
    auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "", "1000001", "10");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 422,
                                            .message = "An empty entry name is not allowed",
                                        });
  }

  SECTION("wrong content-length") {
    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();

    SECTION("empty") {
      auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "");
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error == Error::ContentLengthRequired("Bad or empty content-length"));
    }

    SECTION("negative") {
      auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "-1");
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error == Error::ContentLengthRequired("Negative content-length"));
    }

    SECTION("no number") {
      auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "xxxx");
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error == Error::ContentLengthRequired("Bad or empty content-length"));
    }
    REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error.code == 404);
  }

  SECTION("wrong input") {
    auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10");
    REQUIRE(err == Error::kOk);

    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();

    SECTION("too long") {
      REQUIRE(receiver("1234567890X", true).error == Error{
                                                         .code = 413,
                                                         .message = "Content is bigger than in content-length",
                                                     });
      REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error == Error{
                                                                                .code = 500,
                                                                                .message = "Record is broken",
                                                                            });
    }

    SECTION("too short") {
      REQUIRE(receiver("1234", true).error == Error{
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
    auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", "1000001", {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    REQUIRE(resp.headers["content-type"] == "application/octet-stream");
    REQUIRE(resp.headers["x-reduct-time"] == "1000001");
    REQUIRE(resp.headers["x-reduct-last"] == "1");
    REQUIRE(resp.content_length == 10);

    auto ret = resp.SendData();
    REQUIRE(ret == Error::kOk);
    REQUIRE(ret.result == "1234567890");
  }

  SECTION("bucket doesn't exist") {
    auto [receiver, err] = EntryApi::Read(storage.get(), "XXX", "entry-1", "1000001", {});
    REQUIRE(err == Error::kOk);

    REQUIRE(receiver("", true).error == Error{
                                            .code = 404,
                                            .message = "Bucket 'XXX' is not found",
                                        });
  }

  SECTION("entry doesn't exist") {
    auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "XXX", "1000001", {});
    REQUIRE(err == Error::kOk);

    REQUIRE(receiver("", true).error == Error{
                                            .code = 404,
                                            .message = "Entry 'XXX' is not found",
                                        });
  }

  SECTION("record doesn't exist") {
    auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", "1234567", {});
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 404,
                                            .message = "No records for this timestamp",
                                        });
  }

  SECTION("wrong ts") {
    auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "XXXX", {});
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error ==
            Error{
                .code = 422,
                .message = "Failed to parse 'ts' parameter: XXXX must unix times in microseconds",
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
      auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id);
      REQUIRE(err == Error::kOk);

      auto [resp, recv_err] = receiver("", true);
      REQUIRE(recv_err == Error::kOk);

      REQUIRE(resp.headers["content-type"] == "application/octet-stream");
      REQUIRE(resp.headers["x-reduct-time"] == "1000001");
      REQUIRE(resp.headers["x-reduct-last"] == "0");
      REQUIRE(resp.content_length == 10);

      auto ret = resp.SendData();
      REQUIRE(ret == Error::kOk);
      REQUIRE(ret.result == "1234567890");
    }
    {
      auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id);
      REQUIRE(err == Error::kOk);

      auto [resp, recv_err] = receiver("", true);
      REQUIRE(recv_err == Error::kOk);

      REQUIRE(resp.headers["content-type"] == "application/octet-stream");
      REQUIRE(resp.headers["x-reduct-time"] == "2000001");
      REQUIRE(resp.headers["x-reduct-last"] == "1");
      REQUIRE(resp.content_length == 4);

      auto ret = resp.SendData();
      REQUIRE(ret == Error::kOk);
      REQUIRE(ret.result == "abcd");
    }

    {
      auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id);
      REQUIRE(err == Error::kOk);

      REQUIRE(receiver("", true).error ==
              Error{.code = 404, .message = fmt::format("Query id={} doesn't exist. It expired or was finished", id)});
    }
  }

  SECTION("id doesnt' exist") {
    auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, "2");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 404,
                                            .message = "Query id=2 doesn't exist. It expired or was finished",
                                        });
  }

  SECTION("wrong id") {
    auto [receiver, err] = EntryApi::Read(storage.get(), "bucket", "entry-1", {}, "XXX2");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 422,
                                            .message = "Failed to parse 'id' parameter: XXX2 must be unsigned integer",
                                        });
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
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    auto [out, out_err] = resp.SendData();
    REQUIRE(out_err == Error::kOk);
    JsonStringToMessage(out, &info);

    REQUIRE(entry->Next(info.id()).error == Error::kOk);
    REQUIRE(entry->Next(info.id()).error == Error::kOk);
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok start") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000002", {}, {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    JsonStringToMessage(resp.SendData().result, &info);

    REQUIRE(entry->Next(info.id()).result.reader->timestamp() == Time() + us(2000001));
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok stop") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, "1000002", {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    JsonStringToMessage(resp.SendData().result, &info);

    REQUIRE(entry->Next(info.id()).result.reader->timestamp() == Time() + us(1000001));
    REQUIRE(entry->Next(info.id()).error.code == 404);
  }

  SECTION("ok timeinterval") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000003", "1000004", {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    JsonStringToMessage(resp.SendData().result, &info);

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
    auto [receiver, err] = EntryApi::Query(storage.get(), "XXX", "entry-1", {}, {}, {});
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 404,
                                            .message = "Bucket 'XXX' is not found",
                                        });
  }

  SECTION("entry doesn't exist") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "XXX", {}, {}, {});
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true).error == Error{
                                            .code = 404,
                                            .message = "Entry 'XXX' is not found",
                                        });
  }

  SECTION("wrong parameters") {
    {
      auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "XXX", {}, {});
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error ==
              Error{
                  .code = 422,
                  .message = "Failed to parse 'start_timestamp' parameter: XXX must unix times in microseconds",
              });
    }
    {
      auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, "XXX", {});
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error ==
              Error{
                  .code = 422,
                  .message = "Failed to parse 'stop_timestamp' parameter: XXX must unix times in microseconds",
              });
    }
    {
      auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, "XXX");
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error ==
              Error{
                  .code = 422,
                  .message = "Failed to parse 'ttl' parameter: XXX must be unsigned integer",
              });
    }
    {
      auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, "XXX");
      REQUIRE(err == Error::kOk);
      REQUIRE(receiver("", true).error ==
              Error{
                  .code = 422,
                  .message = "Failed to parse 'ttl' parameter: XXX must be unsigned integer",
              });
    }
  }
}
