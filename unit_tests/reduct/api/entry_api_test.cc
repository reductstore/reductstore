// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
using reduct::storage::IEntry;
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
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10").error ==
              Error::Conflict("A record with timestamp 1000001 already exists"));
    }
  }

  SECTION("wrong ts") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "XXXX", "10").error ==
            Error::UnprocessableEntity("Failed to parse 'ts' parameter: XXXX must unix times in microseconds"));
  }

  SECTION("negative ts") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "-100", "10").error ==

            Error::UnprocessableEntity("Failed to parse 'ts' parameter: -100 must be positive"));
  }

  SECTION("empty entry") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "", "1000001", "10").error ==
            Error::UnprocessableEntity("An empty entry name is not allowed"));
  }

  SECTION("wrong content-length") {
    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();

    SECTION("empty") {
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "").error ==
              Error::ContentLengthRequired("Bad or empty content-length"));
    }

    SECTION("negative") {
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "-1").error ==
              Error::ContentLengthRequired("Negative content-length"));
    }

    SECTION("no number") {
      REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "xxxx").error ==
              Error::ContentLengthRequired("Bad or empty content-length"));
    }

    REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error.code == 404);
  }

  SECTION("wrong input") {
    auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10");
    REQUIRE(err == Error::kOk);

    auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();

    SECTION("too long") {
      REQUIRE(receiver("1234567890X", true).error == Error::BadRequest("Content is bigger than in content-length"));
      REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error == Error::InternalError("Record is broken"));
    }

    SECTION("too short") {
      REQUIRE(receiver("1234", true).error == Error::BadRequest("Content is smaller than in content-length"));
      REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).error == Error::InternalError("Record is broken"));
    }
  }
}

TEST_CASE("EntryAPI::Write should write data with labels") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  const IEntry::LabelMap labels = {
      {"label1", "value1"},
      {"label2", "value2"},
  };

  auto [receiver, err] = EntryApi::Write(storage.get(), "bucket", "entry-1", "1000001", "10", labels);
  REQUIRE(err == Error::kOk);

  REQUIRE(receiver("12345", false).error.code == 100);

  auto [resp, resp_err] = receiver("67890", true);
  REQUIRE(resp_err == Error::kOk);

  REQUIRE(resp.headers.empty());
  REQUIRE(resp.content_length == 0);

  auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
  REQUIRE(entry->BeginRead(reduct::core::Time() + us(1000001)).result->labels() == labels);
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
    REQUIRE(EntryApi::Read(storage.get(), "XXX", "entry-1", "1000001", {}).error ==
            Error::NotFound("Bucket 'XXX' is not found"));
  }

  SECTION("entry doesn't exist") {
    REQUIRE(EntryApi::Read(storage.get(), "bucket", "XXX", "1000001", {}).error ==
            Error::NotFound("Entry 'XXX' is not found"));
  }

  SECTION("record doesn't exist") {
    REQUIRE(EntryApi::Read(storage.get(), "bucket", "entry-1", "1234567", {}).error ==
            Error::NotFound("No records for this timestamp"));
  }

  SECTION("wrong ts") {
    REQUIRE(EntryApi::Write(storage.get(), "bucket", "entry-1", "XXXX", {}).error ==

            Error::UnprocessableEntity("Failed to parse 'ts' parameter: XXXX must unix times in microseconds"));
  }
}

TEST_CASE("EntryApi::Read should read data in chunks with query id") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  auto entry = storage->GetBucket("bucket").result.lock()->GetOrCreateEntry("entry-1").result.lock();
  REQUIRE(WriteOne(*entry, "1234567890", Time() + us(1000001), {{"label1", "value1"}, {"label2", "value2"}}) ==
          Error::kOk);
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
      REQUIRE(resp.headers["x-reduct-label-label1"] == "value1");
      REQUIRE(resp.headers["x-reduct-label-label2"] == "value2");
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

    REQUIRE(EntryApi::Read(storage.get(), "bucket", "entry-1", {}, id).error ==
            Error::NotFound(fmt::format("Query id={} doesn't exist. It expired or was finished", id)));
  }

  SECTION("id doesnt' exist") {
    REQUIRE(EntryApi::Read(storage.get(), "bucket", "entry-1", {}, "2").error ==
            Error::NotFound("Query id=2 doesn't exist. It expired or was finished"));
  }

  SECTION("wrong id") {
    REQUIRE(EntryApi::Read(storage.get(), "bucket", "entry-1", {}, "XXX2").error ==
            Error::UnprocessableEntity("Failed to parse 'id' parameter: XXX2 must be unsigned integer"));
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
    REQUIRE(entry->Next(info.id()).error.code == Error::kNotFound);
  }

  SECTION("ok start") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000002", {}, {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    JsonStringToMessage(resp.SendData().result, &info);

    REQUIRE(entry->Next(info.id()).result.reader->timestamp() == Time() + us(2000001));
    REQUIRE(entry->Next(info.id()).error.code == Error::kNotFound);
  }

  SECTION("ok stop") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", {}, "1000002", {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    JsonStringToMessage(resp.SendData().result, &info);

    REQUIRE(entry->Next(info.id()).result.reader->timestamp() == Time() + us(1000001));
    REQUIRE(entry->Next(info.id()).error.code == Error::kNotFound);
  }

  SECTION("ok timeinterval") {
    auto [receiver, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000003", "1000004", {});
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    JsonStringToMessage(resp.SendData().result, &info);

    REQUIRE(entry->Next(info.id()).error.code == Error::kNoContent);  // no content
    REQUIRE(entry->Next(info.id()).error.code == Error::kNotFound);
  }

  SECTION("ok ttl") {
    auto [resp, err] = EntryApi::Query(storage.get(), "bucket", "entry-1", "1000003", "1000004", "1");
    REQUIRE(err == Error::kOk);

    std::this_thread::sleep_for(us(1'000'000));
    REQUIRE(entry->Next(info.id()).error.code == Error::kNotFound);
  }

  SECTION("bucket doesn't exist") {
    REQUIRE(EntryApi::Query(storage.get(), "XXX", "entry-1", {}, {}, {}).error ==
            Error::NotFound("Bucket 'XXX' is not found"));
  }

  SECTION("entry doesn't exist") {
    REQUIRE(EntryApi::Query(storage.get(), "bucket", "XXX", {}, {}, {}).error ==
            Error::NotFound("Entry 'XXX' is not found"));
  }

  SECTION("wrong parameters") {
    REQUIRE(
        EntryApi::Query(storage.get(), "bucket", "entry-1", "XXX", {}, {}).error ==
        Error::UnprocessableEntity("Failed to parse 'start_timestamp' parameter: XXX must unix times in microseconds"));
    REQUIRE(
        EntryApi::Query(storage.get(), "bucket", "entry-1", {}, "XXX", {}).error ==
        Error::UnprocessableEntity("Failed to parse 'stop_timestamp' parameter: XXX must unix times in microseconds"));
    REQUIRE(EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, "XXX").error ==
            Error::UnprocessableEntity("Failed to parse 'ttl' parameter: XXX must be unsigned integer"));
    REQUIRE(EntryApi::Query(storage.get(), "bucket", "entry-1", {}, {}, "XXX").error ==
            Error::UnprocessableEntity("Failed to parse 'ttl' parameter: XXX must be unsigned integer"));
  }
}
