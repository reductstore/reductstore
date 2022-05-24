// Copyright 2021-2022 Alexey Timin

#include "reduct/storage/bucket.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::core::Error;
using reduct::proto::api::BucketSettings;
using reduct::storage::IBucket;
using reduct::storage::IEntry;

using std::chrono::seconds;
namespace fs = std::filesystem;

TEST_CASE("storage::Bucket should create folder", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  BucketSettings settings;
  auto bucket = IBucket::Build(dir_path / "bucket", settings);

  REQUIRE(bucket);
  REQUIRE(fs::exists(dir_path));

  SECTION("error, if directory already exist") { REQUIRE_FALSE(IBucket::Build(dir_path / "bucket", settings)); }

  SECTION("return nullptr if something got wrong") {
    fs::create_directories("some/path");
    REQUIRE_FALSE(IBucket::Build("some/path", settings));
  }
}

TEST_CASE("storage::Bucket should restore from folder", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  BucketSettings settings;
  settings.set_max_block_size(100);
  settings.set_quota_type(BucketSettings::FIFO);
  settings.set_quota_size(1000);
  auto bucket = IBucket::Build(dir_path / "bucket", settings);

  REQUIRE(bucket->GetOrCreateEntry("entry1").error == Error::kOk);

  auto restored_bucket = IBucket::Restore(dir_path / "bucket");
  REQUIRE(restored_bucket->GetInfo() == bucket->GetInfo());
  REQUIRE(restored_bucket->GetSettings() == bucket->GetSettings());
  REQUIRE(restored_bucket->GetEntryList().size() == bucket->GetEntryList().size());
  REQUIRE(restored_bucket->GetEntryList()[0] == bucket->GetEntryList()[0]);

  SECTION("empty folder") {
    fs::create_directory(dir_path / "empty_folder");
    REQUIRE_FALSE(IBucket::Restore(dir_path / "empty_folder"));
  }
}

TEST_CASE("storage::Bucket should create get or create entry", "[bucket][entry]") {
  auto bucket = IBucket::Build(BuildTmpDirectory() / "bucket");

  SECTION("create a new entry") {
    auto [entry, err] = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(err == Error::kOk);
    REQUIRE(entry.lock());
  }

  SECTION("get an existing entry") {
    auto ref = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(ref.error == Error::kOk);
    REQUIRE(ref.entry.lock()->GetInfo().record_count() == 0);
    REQUIRE(ref.entry.lock()->BeginWrite(IEntry::Time::clock::now(), 9).result->Write("some_blob") == Error::kOk);

    ref = bucket->GetOrCreateEntry("entry_1");
    REQUIRE(ref.error == Error::kOk);
    REQUIRE(ref.entry.lock()->GetInfo().record_count() == 1);
  }
}

TEST_CASE("storage::Bucket should remove all entries", "[bucket]") {
  auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build(dir_path / "bucket");

  REQUIRE(bucket->GetOrCreateEntry("entry_1").error == Error::kOk);
  REQUIRE(bucket->GetOrCreateEntry("entry_2").error == Error::kOk);
  REQUIRE(bucket->GetInfo().entry_count() == 2);

  REQUIRE(bucket->Clean() == Error::kOk);
  REQUIRE(bucket->GetInfo().entry_count() == 0);
  REQUIRE_FALSE(fs::exists(dir_path / "bucket" / "entry_1"));
  REQUIRE_FALSE(fs::exists(dir_path / "bucket" / "entry_2"));
}

TEST_CASE("storage::Bucket should keep quota", "[bucket]") {
  BucketSettings settings;
  settings.set_max_block_size(100);
  settings.set_quota_type(BucketSettings::FIFO);
  settings.set_quota_size(1000);
  const auto path = BuildTmpDirectory();
  auto bucket = IBucket::Build(path / "bucket", std::move(settings));

  auto entry1 = bucket->GetOrCreateEntry("entry_1").entry.lock();
  auto entry2 = bucket->GetOrCreateEntry("entry_2").entry.lock();

  const auto ts = IEntry::Time();
  std::string blob(400, 'x');

  SECTION("3 big blobs 3*400 should be shrunk to 2") {
    REQUIRE(entry1->BeginWrite(ts + seconds(1), blob.size()).result->Write(blob) == Error::kOk);
    REQUIRE(entry2->BeginWrite(ts + seconds(2), blob.size()).result->Write(blob) == Error::kOk);
    REQUIRE(entry1->BeginWrite(ts + seconds(3), blob.size()).result->Write(blob) == Error::kOk);

    REQUIRE(bucket->KeepQuota() == Error::kOk);
    REQUIRE(entry1->GetInfo().record_count() == 1);
    REQUIRE(entry2->GetInfo().record_count() == 1);

    REQUIRE(entry1->BeginRead(ts + seconds(1)).error.code == 404);
    REQUIRE(entry1->BeginRead(ts + seconds(3)).error == Error::kOk);
    REQUIRE(entry2->BeginRead(ts + seconds(2)).error == Error::kOk);

    SECTION("the same state after restoring") {
      auto info = bucket->GetInfo();

      bucket = IBucket::Restore(path / "bucket");
      REQUIRE(bucket);
      REQUIRE(info == bucket->GetInfo());
    }
  }

  SECTION("should remove entry if no blocks") {
    // TODO(Alexey Timin): Clean code
    std::string little_chunk("little_chunk");
    REQUIRE(entry1->BeginWrite(ts + seconds(1), little_chunk.size()).result->Write(little_chunk) == Error::kOk);
    REQUIRE(entry2->BeginWrite(ts + seconds(2), blob.size()).result->Write(blob) == Error::kOk);
    REQUIRE(entry2->BeginWrite(ts + seconds(3), blob.size()).result->Write(blob) == Error::kOk);
    REQUIRE(entry2->BeginWrite(ts + seconds(4), blob.size()).result->Write(blob) == Error::kOk);

    REQUIRE(bucket->KeepQuota() == Error::kOk);

    REQUIRE(entry1->BeginRead(ts + seconds(1)).error.code == 404);
    REQUIRE(entry2->BeginRead(ts + seconds(2)).error.code == 404);

    REQUIRE(bucket->GetEntryList().size() == 1);
  }

  SECTION("should work with empty entry-2") {
    REQUIRE(entry1->BeginWrite(ts + seconds(1), blob.size()).result->Write(blob) == Error::kOk);
    REQUIRE(entry1->BeginWrite(ts + seconds(2), blob.size()).result->Write(blob) == Error::kOk);
    REQUIRE(entry1->BeginWrite(ts + seconds(3), blob.size()).result->Write(blob) == Error::kOk);

    REQUIRE(bucket->KeepQuota() == Error::kOk);
    REQUIRE(entry1->GetInfo().record_count() == 2);
  }
}

TEST_CASE("storage::Bucket should change quota settings and save it", "[bucket]") {
  const auto dir_path = BuildTmpDirectory();
  auto bucket = IBucket::Build(dir_path / "bucket");

  BucketSettings settings;
  settings.set_max_block_size(600);
  settings.set_quota_type(BucketSettings::FIFO);
  settings.set_quota_size(1000);

  REQUIRE(bucket->SetSettings(settings) == Error::kOk);
  REQUIRE(bucket->GetSettings() == settings);

  bucket = IBucket::Restore(dir_path / "bucket");
  REQUIRE(bucket->GetSettings() == settings);
}
