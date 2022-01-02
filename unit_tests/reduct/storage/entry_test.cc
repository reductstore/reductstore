// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::core::Error;
using reduct::storage::IEntry;
using std::chrono::seconds;

TEST_CASE("storage::Entry should record file to a block", "[entry]") {
  auto entry = IEntry::Build({
      .name = "entry_1",
      .path = BuildTmpDirectory(),
      .min_block_size = 1024,
  });

  REQUIRE(entry);

  const auto time = IEntry::Time::clock::now();
  auto err = entry->Write("some_data", time);
  REQUIRE_FALSE(err);

  SECTION("one point") {
    auto ret = entry->Read(time);
    REQUIRE(ret == IEntry::ReadResult{"some_data", Error{}, time});
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 1, .bytes = 11});
  }

  SECTION("few points") {
    REQUIRE_FALSE(entry->Write("other_data1", time + seconds(5)));
    REQUIRE_FALSE(entry->Write("other_data2", time + seconds(10)));
    REQUIRE_FALSE(entry->Write("other_data3", time + seconds(15)));

    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 1, .record_count = 4, .bytes = 50});

    auto ret = entry->Read(time);
    REQUIRE(ret.data == "some_data");

    ret = entry->Read(time + seconds(15));
    REQUIRE(ret.data == "other_data3");
  }

  SECTION("error 404 if request out of  interval") {
    auto bad_result = entry->Read(time - seconds(10));
    REQUIRE(bad_result.error == Error{.code = 404, .message = "No records for this timestamp"});
  }
}

TEST_CASE("storage::Entry should create a new block if the current > min_block_size", "[entry]") {
  auto entry = IEntry::Build({
      .name = "entry_1",
      .path = BuildTmpDirectory(),
      .min_block_size = 100,
  });

  REQUIRE(entry);

  const auto time = IEntry::Time::clock::now();
  auto err = entry->Write(std::string(100, 'c'), time);
  REQUIRE_FALSE(err);

  SECTION("one point") {
    auto ret = entry->Read(time);
    REQUIRE(ret == IEntry::ReadResult{std::string(100, 'c'), Error{}, time});
    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 2, .record_count = 1, .bytes = 102});
  }

  SECTION("two points in different blocks") {
    REQUIRE_FALSE(entry->Write("other_data1", time + seconds(5)));

    REQUIRE(entry->GetInfo() == IEntry::Info{.block_count = 2, .record_count = 2, .bytes = 115});

    auto ret = entry->Read(time + seconds(5));
    REQUIRE(ret.data == "other_data1");
  }
}
