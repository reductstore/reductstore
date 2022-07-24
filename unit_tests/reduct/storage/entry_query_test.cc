// Copyright 2022 Alexey Timin

#include <catch2/catch.hpp>
#include <google/protobuf/util/time_util.h>

#include <filesystem>

#include "reduct/helpers.h"
#include "reduct/storage/entry.h"

using reduct::ReadOne;
using reduct::WriteOne;
using reduct::core::Error;
using reduct::storage::IEntry;
using reduct::storage::query::IQuery;

using google::protobuf::util::TimeUtil;

using std::chrono::seconds;
namespace fs = std::filesystem;

const auto kName = "entry_1";

static auto MakeDefaultOptions() {
  return IEntry::Options{
      .max_block_size = 100,
      .max_block_records = 1024,
  };
}

static const auto kTimestamp = IEntry::Time() + std::chrono::microseconds(10'100'200);  // to check um precision

TEST_CASE("storage::Entry should query records", "[entry][query]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  const IQuery::Options kDefaultOptions = {.ttl = seconds(1)};

  SECTION("empty entry") {
    auto [start, stop] =
        GENERATE(std::make_pair(std::make_optional(kTimestamp), std::make_optional(kTimestamp + seconds(1))),
                 std::make_pair(std::make_optional(kTimestamp), std::nullopt),
                 std::make_pair(std::nullopt, std::make_optional(kTimestamp + seconds(1))));

    auto [q_id, q_err] = entry->Query(start, stop, kDefaultOptions);
    REQUIRE(q_err == Error::kOk);

    auto [record, err] = entry->Next(q_id);
    REQUIRE(err == Error{.code = 202, .message = "No Content"});
  }

  SECTION("wrong ID") {
    auto [record, err] = entry->Next(10000);
    REQUIRE(err == Error{.code = 404, .message = "Query id=10000 doesn't exist. It expired or was finished"});
  }

  SECTION("some records in few blocks") {
    const std::string blob(entry->GetOptions().max_block_size, 'x');
    REQUIRE(WriteOne(*entry, blob, kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(2)) == Error::kOk);

    SECTION("without overlap") {
      auto [id, err] = entry->Query(kTimestamp, kTimestamp + seconds(2), kDefaultOptions);

      REQUIRE(err == Error::kOk);

      auto ret = entry->Next(id);
      REQUIRE(ret.error == Error::kOk);
      REQUIRE(ret.result == IQuery::NextRecord{.time = kTimestamp, .size = 100, .last = false});

      ret = entry->Next(id);
      REQUIRE(ret.error == Error::kOk);
      REQUIRE(ret.result == IQuery::NextRecord{.time = kTimestamp + seconds(1), .size = 100, .last = true});

      err = entry->Next(id);
      REQUIRE(err == Error{
                         .code = 404,
                         .message = fmt::format("Query id={} doesn't exist. It expired or was finished", id),
                     });
    }

    SECTION("with overlap and default values") {
      auto [start, stop] = GENERATE(
          std::make_pair(std::make_optional(kTimestamp - seconds(1)), std::make_optional(kTimestamp + seconds(4))),
          std::make_pair(std::make_optional(kTimestamp - seconds(1)), std::nullopt),
          std::make_pair(std::nullopt, std::make_optional(kTimestamp + seconds(4))));

      auto [id, err] = entry->Query(start, stop, kDefaultOptions);

      auto ret = entry->Next(id);
      REQUIRE(ret.error == Error::kOk);
      REQUIRE(ret.result.last == false);

      ret = entry->Next(id);
      REQUIRE(ret.error == Error::kOk);
      REQUIRE(ret.result.last == false);

      ret = entry->Next(id);
      REQUIRE(ret.error == Error::kOk);
      REQUIRE(ret.result.last == true);

      REQUIRE(entry->Next(id).error.code == 404);
    }
  }

  SECTION("some records in one block") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [id, err] = entry->Query(kTimestamp, kTimestamp + seconds(2), kDefaultOptions);

    REQUIRE(err == Error::kOk);
    auto ret = entry->Next(id);
    REQUIRE(ret.error == Error::kOk);
    REQUIRE(ret.result == IQuery::NextRecord{.time = kTimestamp, .size = 4, .last = false});

    ret = entry->Next(id);
    REQUIRE(ret.error == Error::kOk);
    REQUIRE(ret.result == IQuery::NextRecord{.time = kTimestamp + seconds(1), .size = 4, .last = true});
  }

  SECTION("should send records sorted") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [id, err] = entry->Query(kTimestamp, kTimestamp + seconds(2), kDefaultOptions);

    REQUIRE(err == Error::kOk);
    auto ret = entry->Next(id);
    REQUIRE(ret.error == Error::kOk);
    REQUIRE(ret.result == IQuery::NextRecord{.time = kTimestamp, .size = 4, .last = false});

    ret = entry->Next(id);
    REQUIRE(ret.error == Error::kOk);
    REQUIRE(ret.result == IQuery::NextRecord{.time = kTimestamp + seconds(1), .size = 4, .last = true});
  }

  SECTION("extreme cases") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    SECTION("request data before first record") {
      auto [id, err] = entry->Query(kTimestamp - seconds(2), kTimestamp - seconds(1), kDefaultOptions);
      REQUIRE(err == Error::kOk);
      REQUIRE(entry->Next(id).error == Error{.code = 202, .message = "No Content"});
    }

    SECTION("request data after last record") {
      auto [id, err] = entry->Query(kTimestamp + seconds(3), kTimestamp + seconds(4), kDefaultOptions);
      REQUIRE(err == Error::kOk);
      REQUIRE(entry->Next(id).error == Error{.code = 202, .message = "No Content"});
    }

    SECTION("if there is overlap it is ok") {
      auto [id, err] = entry->Query(kTimestamp + seconds(1), kTimestamp + seconds(4), kDefaultOptions);
      REQUIRE(entry->Next(id).result.time == kTimestamp + seconds(1));
      REQUIRE(entry->Next(id).result.time == kTimestamp + seconds(2));

      id = entry->Query(kTimestamp - seconds(1), kTimestamp + seconds(2), kDefaultOptions).result;
      REQUIRE(entry->Next(id).result.time == kTimestamp);
      REQUIRE(entry->Next(id).result.time == kTimestamp + seconds(1));
    }

    SECTION("if timestamp between two blocks") {
      const std::string blob(entry->GetOptions().max_block_size / 2 + 10, 'x');

      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(3)) == Error::kOk);
      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(4)) == Error::kOk);

      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(10)) == Error::kOk);
      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(11)) == Error::kOk);

      auto id = entry->Query(kTimestamp + seconds(5), kTimestamp + seconds(12), kDefaultOptions).result;
      REQUIRE(entry->Next(id).error == Error{.code = 202, .message = "No Content"});
    }
  }
}

TEST_CASE("storage::Entry should have TTL", "[entry][query]") {
  auto entry = IEntry::Build(kName, BuildTmpDirectory(), MakeDefaultOptions());
  REQUIRE(entry);

  const IQuery::Options kDefaultOptions = {.ttl = seconds(1)};

  REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
  REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
  REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

  SECTION("expires in TTL seconds") {
    auto [id, err] = entry->Query(kTimestamp, kTimestamp + seconds(2), kDefaultOptions);

    std::this_thread::sleep_for(kDefaultOptions.ttl);
    err = entry->Next(id);
    REQUIRE(err == Error{
                       .code = 404,
                       .message = fmt::format("Query id={} doesn't exist. It expired or was finished", id),
                   });
  }

  SECTION("expires in TTL seconds after Next") {
    auto [id, err] = entry->Query(kTimestamp, kTimestamp + seconds(2), kDefaultOptions);

    std::this_thread::sleep_for(kDefaultOptions.ttl - std::chrono::milliseconds(500));
    err = entry->Next(id);
    REQUIRE(err == Error::kOk);


    std::this_thread::sleep_for(kDefaultOptions.ttl);
    err = entry->Next(id);
    REQUIRE(err == Error{
                       .code = 404,
                       .message = fmt::format("Query id={} doesn't exist. It expired or was finished", id),
                   });
  }
}
