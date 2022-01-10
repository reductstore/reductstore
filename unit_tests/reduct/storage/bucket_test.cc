// Copyright 2021-2022 Alexey Timin

#include "reduct/storage/bucket.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::core::Error;
using reduct::storage::IBucket;
using reduct::storage::IEntry;

using std::chrono::seconds;
namespace fs = std::filesystem;

TEST_CASE("storage::Bucket should create folder", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build({.name = "bucket", .path = dir_path});

  REQUIRE(bucket);
  REQUIRE(fs::exists(dir_path));

  SECTION("error, if directory already exist") { REQUIRE_FALSE(IBucket::Build({.name = "bucket", .path = dir_path})); }

  SECTION("return nullptr if something got wrong") {
    REQUIRE_FALSE(IBucket::Build({.name = "", .path = "/non-existing/path"}));
  }

  SECTION("name cannot be empty") { REQUIRE_FALSE(IBucket::Build({.name = "", .path = dir_path})); }
}

TEST_CASE("storage::Bucket should restore from folder", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build({
      .name = "bucket",
      .path = dir_path,
      .max_block_size = 100,
      .quota = IBucket::QuotaOptions{.type = IBucket::QuotaType::kFifo, .size = 1000},
  });

  REQUIRE(bucket->GetOrCreateEntry("entry1").error == Error::kOk);

  auto restored_bucket = IBucket::Restore(dir_path / "bucket");
  REQUIRE(restored_bucket->GetInfo() == bucket->GetInfo());
  REQUIRE(restored_bucket->GetOptions() == bucket->GetOptions());

  SECTION("empty folder") {
    fs::create_directory(dir_path / "empty_folder");
    REQUIRE_FALSE(IBucket::Restore(dir_path / "empty_folder"));
  }
}

TEST_CASE("storage::Bucket should create get or create entry", "[bucket][entry]") {
  auto bucket = IBucket::Build({.name = "bucket", .path = BuildTmpDirectory()});

  SECTION("create a new entry") {
    auto [entry, err] = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(err == Error::kOk);
    REQUIRE(entry.lock());
  }

  SECTION("get an existing entry") {
    auto ref = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(ref.error == Error::kOk);
    REQUIRE(ref.entry.lock()->GetInfo().record_count == 0);
    REQUIRE(ref.entry.lock()->Write("some_blob", IEntry::Time::clock::now()) == Error::kOk);

    ref = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(ref.error == Error::kOk);
    REQUIRE(ref.entry.lock()->GetInfo().record_count == 1);
  }
}

TEST_CASE("storage::Bucket should remove all entries", "[bucket]") {
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

TEST_CASE("storage::Bucket should keep quota", "[bucket]") {
  auto bucket = IBucket::Build({
      .name = "bucket",
      .path = BuildTmpDirectory(),
      .max_block_size = 100,
      .quota = IBucket::QuotaOptions{.type = IBucket::QuotaType::kFifo, .size = 1000},
  });

  auto entry1 = bucket->GetOrCreateEntry("entry_1").entry.lock();
  auto entry2 = bucket->GetOrCreateEntry("entry_2").entry.lock();

  const auto ts = IEntry::Time();
  std::string blob(700, 'x');

  SECTION("3 big blobs 3*700 should be shrunk to 1") {
    REQUIRE(entry1->Write(blob, ts + seconds(1)) == Error::kOk);
    REQUIRE(entry2->Write(blob, ts + seconds(2)) == Error::kOk);
    REQUIRE(entry1->Write(blob, ts + seconds(3)) == Error::kOk);

    REQUIRE(bucket->GetInfo().record_count == 3);
    REQUIRE(bucket->KeepQuota() == Error::kOk);
    REQUIRE(bucket->GetInfo().record_count == 1);

    REQUIRE(entry1->Read(ts + seconds(1)).error.code == 404);
    REQUIRE(entry2->Read(ts + seconds(2)).error.code == 404);

    SECTION("the same state after restoring") {
      auto info = bucket->GetInfo();

      bucket = IBucket::Restore(bucket->GetOptions().path / "bucket");
      REQUIRE(bucket);
      REQUIRE(info == bucket->GetInfo());
    }
  }

  SECTION("should clean current block") {
    REQUIRE(entry1->Write("little_fist_chunk", ts + seconds(1)) == Error::kOk);
    REQUIRE(entry2->Write(blob, ts + seconds(2)) == Error::kOk);
    REQUIRE(entry2->Write(blob, ts + seconds(3)) == Error::kOk);

    REQUIRE(bucket->KeepQuota() == Error::kOk);

    REQUIRE(entry1->Read(ts + seconds(1)).error.code == 404);
    REQUIRE(entry2->Read(ts + seconds(2)).error.code == 404);
  }
}

TEST_CASE("storage::Bucket should change quota settings and save it", "[bucket]") {
  const auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build({
      .name = "bucket",
      .path = dir_path,
      .max_block_size = 100,
  });

  IBucket::Options options = {.max_block_size = 600, .quota = {.type = IBucket::kFifo, .size = 1000}};

  REQUIRE(bucket->SetOptions(options) == Error::kOk);
  options.name = "bucket";
  options.path = dir_path;
  REQUIRE(bucket->GetOptions() == options);

  bucket = IBucket::Restore(dir_path / "bucket");
  REQUIRE(bucket->GetOptions() == options);
}