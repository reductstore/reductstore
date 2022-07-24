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
    }

    //    SECTION("with overlap") {
    //      auto [records, err] = entry->List(kTimestamp - seconds(1), kTimestamp + seconds(4));
    //      REQUIRE(err == Error::kOk);
    //      REQUIRE(records.size() == 3);
    //    }
  }
#if 0

  SECTION("some records in one block") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 4});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 4});
  }

  SECTION("list should be sorted") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    auto [records, err] = entry->List(kTimestamp, kTimestamp + seconds(2));

    REQUIRE(err == Error::kOk);
    REQUIRE(records.size() == 2);
    REQUIRE(records[0] == IEntry::RecordInfo{.time = kTimestamp, .size = 4});
    REQUIRE(records[1] == IEntry::RecordInfo{.time = kTimestamp + seconds(1), .size = 4});
  }

  SECTION("extreme cases") {
    REQUIRE(WriteOne(*entry, "blob", kTimestamp) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(1)) == Error::kOk);
    REQUIRE(WriteOne(*entry, "blob", kTimestamp + seconds(2)) == Error::kOk);

    SECTION("request data before first record") {
      auto [records, err] = entry->List(kTimestamp - seconds(2), kTimestamp - seconds(1));
      REQUIRE(err == Error{.code = 404, .message = "No records for time interval"});
      REQUIRE(records.empty());
    }

    SECTION("request data after last record") {
      auto [records, err] = entry->List(kTimestamp + seconds(3), kTimestamp + seconds(4));
      REQUIRE(err == Error{.code = 404, .message = "No records for time interval"});
      REQUIRE(records.empty());
    }

    SECTION("if there is overlap it is ok") {
      auto records = entry->List(kTimestamp + seconds(1), kTimestamp + seconds(4)).result;
      REQUIRE(records.size() == 2);
      REQUIRE(records[0].time == kTimestamp + seconds(1));
      REQUIRE(records[1].time == kTimestamp + seconds(2));

      records = entry->List(kTimestamp - seconds(1), kTimestamp + seconds(2)).result;
      REQUIRE(records.size() == 2);
      REQUIRE(records[0].time == kTimestamp);
      REQUIRE(records[1].time == kTimestamp + seconds(1));
    }

    SECTION("if timestamp between two blocks") {
      const std::string blob(entry->GetOptions().max_block_size / 2 + 10, 'x');

      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(3)) == Error::kOk);
      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(4)) == Error::kOk);

      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(10)) == Error::kOk);
      REQUIRE(WriteOne(*entry, blob, kTimestamp + seconds(11)) == Error::kOk);

      REQUIRE(entry->List(kTimestamp + seconds(5), kTimestamp + seconds(12)).error == Error::kOk);
    }
  }
#endif
}