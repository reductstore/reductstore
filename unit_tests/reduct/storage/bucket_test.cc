// Copyright 2021 Alexey Timin

#include "reduct/storage/bucket.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::storage::IBucket;

namespace fs = std::filesystem;

TEST_CASE("storage::Bucket should create folder", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build({.name = "bucket", .path = dir_path});

  REQUIRE(bucket);
  REQUIRE(fs::exists(dir_path));

  SECTION("it is ok, if directory already exist") {
    REQUIRE(IBucket::Build({.name = "bucket", .path = dir_path}));
  }

  SECTION("return nullptr if something got wrong") {
    REQUIRE_FALSE(IBucket::Build({.name = "", .path = "/non-existing/path"}));
  }

  SECTION("name cannot be empty") {
    REQUIRE_FALSE(IBucket::Build({.name = "", .path = dir_path}));
  }
}
