// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::core::Error;
using reduct::storage::IEntry;
using std::chrono::seconds;

static auto MakeDefaultOptions() {
  return IEntry::Options{
      .name = "entry_1",
      .path = BuildTmpDirectory(),
      .max_block_size = 100,
  };
}

static const auto kTimestamp = IEntry::Time::clock::now();

TEST_CASE("storage::Entry should record file to a block", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(entry->Write("some_data", kTimestamp) == Error::kOk);

  SECTION("one record") {
    auto ret = entry->Read(kTimestamp);
    REQUIRE(ret == IEntry::ReadResult{"some_data", Error{}, kTimestamp});
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 1, .bytes = 11});
  }

  SECTION("few records") {
    REQUIRE(entry->Write("other_data1", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->Write("other_data2", kTimestamp + seconds(10)) == Error::kOk);
    REQUIRE(entry->Write("other_data3", kTimestamp + seconds(15)) == Error::kOk);

    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 4, .bytes = 50});

    auto ret = entry->Read(kTimestamp);
    REQUIRE(ret.blob == "some_data");

    ret = entry->Read(kTimestamp + seconds(15));
    REQUIRE(ret.blob == "other_data3");
  }

  SECTION("error 404 if request out of  interval") {
    auto bad_result = entry->Read(kTimestamp - seconds(10));
    REQUIRE(bad_result.error == Error{.code = 404, .message = "No records for this timestamp"});
  }
}

TEST_CASE("storage::Entry should create a new block if the current > max_block_size", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(entry->Write(std::string(100, 'c'), kTimestamp) == Error::kOk);

  SECTION("one record") {
    auto ret = entry->Read(kTimestamp);
    REQUIRE(ret == IEntry::ReadResult{std::string(100, 'c'), Error{}, kTimestamp});
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 2, .record_count = 1, .bytes = 102});
  }

  SECTION("two records in different blocks") {
    REQUIRE(entry->Write("other_data1", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 2, .record_count = 2, .bytes = 115});

    auto ret = entry->Read(kTimestamp + seconds(5));
    REQUIRE(ret.blob == "other_data1");
  }
}

TEST_CASE("storage::Entry should write data for random kTimestamp", "[entry]") {
  auto entry = IEntry::Build(MakeDefaultOptions());
  REQUIRE(entry);

  REQUIRE(entry->Write(std::string(100, 'c'), kTimestamp) == Error::kOk);

  SECTION("a record older than first in entry") {
    REQUIRE(entry->Write("belated_data", kTimestamp - seconds(5)) == Error::kOk);
    REQUIRE(entry->Read(kTimestamp - seconds(5)) ==
            IEntry::ReadResult{"belated_data", Error::kOk, kTimestamp - seconds(5)});
  }

  SECTION("a belated record") {
    REQUIRE(entry->GetInfo().block_count == 2);
    REQUIRE(entry->Write("latest_data", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->Write("latest_data", kTimestamp + seconds(15)) == Error::kOk);
    REQUIRE(entry->Write("belated_data", kTimestamp + seconds(10)) == Error::kOk);

    REQUIRE(entry->Read(kTimestamp + seconds(10)) ==
            IEntry::ReadResult{"belated_data", Error::kOk, kTimestamp + seconds(10)});
  }
}

TEST_CASE("storage::Entry should restore itself from descriptors", "[entry]") {
  const auto options = MakeDefaultOptions();
  auto entry = IEntry::Build(options);
  REQUIRE(entry);

  REQUIRE(entry->Write("some_data", kTimestamp) == Error::kOk);

  entry = IEntry::Restore(options.path / options.name);
  REQUIRE(entry->GetOptions() == options);

  REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 1, .bytes = 11});

  SECTION("should work ok after restoring") {
    REQUIRE(entry->Write("next_data", kTimestamp + seconds(5)) == Error::kOk);
    REQUIRE(entry->Read(kTimestamp + seconds(5)) ==
            IEntry::ReadResult{.blob = "next_data", .error = Error::kOk, .time = kTimestamp + seconds(5)});
  }
}
