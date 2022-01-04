// Copyright 2021 Alexey Timin

#include "reduct/storage/bucket.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::core::Error;
using reduct::storage::IBucket;
using reduct::storage::IEntry;

namespace fs = std::filesystem;

TEST_CASE("storage::Bucket should create folder", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build({.name = "bucket", .path = dir_path});

  REQUIRE(bucket);
  REQUIRE(fs::exists(dir_path));

  SECTION("it is ok, if directory already exist") { REQUIRE(IBucket::Build({.name = "bucket", .path = dir_path})); }

  SECTION("return nullptr if something got wrong") {
    REQUIRE_FALSE(IBucket::Build({.name = "", .path = "/non-existing/path"}));
  }

  SECTION("name cannot be empty") { REQUIRE_FALSE(IBucket::Build({.name = "", .path = dir_path})); }
}

TEST_CASE("storage::Bucket should create get or create folder", "[bucket][entry]") {
  auto bucket = IBucket::Build({.name = "bucket", .path = BuildTmpDirectory()});

  SECTION("create a new bucket") {
    auto [entry, err] = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(err == Error::kOk);
    REQUIRE(entry.lock());
  }

  SECTION("get an existing bucket") {
    auto ref = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(ref.error == Error::kOk);
    REQUIRE(ref.entry.lock()->GetInfo().record_count == 0);
    REQUIRE(ref.entry.lock()->Write("some_blob", IEntry::Time::clock::now()) == Error::kOk);

    ref = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(ref.error == Error::kOk);
    REQUIRE(ref.entry.lock()->GetInfo().record_count == 1);
  }
}

TEST_CASE("storage::Bucket should remove all bucket", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build({.name = "bucket", .path = dir_path});

  REQUIRE(bucket->GetOrCreateEntry("entry_1").error == Error::kOk);
  REQUIRE(bucket->GetOrCreateEntry("entry_2").error == Error::kOk);
  REQUIRE(bucket->GetInfo().entry_count == 2);

  REQUIRE(bucket->Clean() == Error::kOk);
  REQUIRE(bucket->GetInfo().entry_count == 0);
  REQUIRE_FALSE(fs::exists(dir_path / "bucket" / "entry_1"));
  REQUIRE_FALSE(fs::exists(dir_path / "bucket" / "entry_2"));
}
