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

  std::string data_to_record = "some_data";
  const auto time = IEntry::Time::clock::now();

  auto err = entry->Write(std::move(data_to_record), time);
  REQUIRE_FALSE(err);

  SECTION("one point") {
    auto [blob, error, ts] = entry->Read(time);
    REQUIRE(blob == "some_data");
    REQUIRE_FALSE(error);
    REQUIRE(ts == time);
  }

  SECTION("few points") {
    REQUIRE_FALSE(entry->Write("other_data1", time + seconds(5)));
    REQUIRE_FALSE(entry->Write("other_data2", time + seconds(10)));
    REQUIRE_FALSE(entry->Write("other_data3", time + seconds(15)));

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