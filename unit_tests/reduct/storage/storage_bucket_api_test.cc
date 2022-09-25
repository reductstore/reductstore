// Copyright 2022 Alexey Timin

#include <catch2/catch.hpp>

#include <filesystem>

#include "reduct/async/task.h"
#include "reduct/helpers.h"
#include "reduct/storage/storage.h"

using reduct::async::Task;
using reduct::core::Error;

using reduct::proto::api::BucketSettings;

using reduct::OnWriteEntry;

using reduct::MakeDefaultBucketSettings;
using reduct::storage::IStorage;

namespace fs = std::filesystem;

TEST_CASE("storage::Storage should create a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  Error err = storage->CreateBucket("bucket", MakeDefaultBucketSettings());
  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    err = storage->CreateBucket("bucket", MakeDefaultBucketSettings());
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    err = storage->CreateBucket("", MakeDefaultBucketSettings());
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}

TEST_CASE("storage::Storage should get a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  const auto settings = MakeDefaultBucketSettings();
  REQUIRE(storage->CreateBucket("bucket", settings) == Error::kOk);

  auto [writer, w_err] =
      OnWriteEntry(storage.get(),
                   {.bucket_name = "bucket", .entry_name = "entry_1", .timestamp = "100000123", .content_length = "8"})
          .Get();
  REQUIRE(w_err == Error::kOk);
  REQUIRE(writer->Write("someblob") == Error::kOk);

  auto [bucket_ptr, err] = storage->GetBucket("bucket");
  REQUIRE(err == Error::kOk);

  auto bucket = bucket_ptr.lock();
  REQUIRE(bucket->GetSettings() == settings);

  REQUIRE(bucket->GetInfo().name() == "bucket");
  REQUIRE(bucket->GetInfo().size() == 8);
  REQUIRE(bucket->GetInfo().entry_count() == 1);
  REQUIRE(bucket->GetInfo().oldest_record() == 100'000'123);
  REQUIRE(bucket->GetInfo().latest_record() == 100'000'123);

  REQUIRE(bucket->GetEntryList().size() == 1);
  REQUIRE(bucket->GetEntryList()[0].name() == "entry_1");
  REQUIRE(bucket->GetEntryList()[0].size() == 8);
  REQUIRE(bucket->GetEntryList()[0].block_count() == 1);
  REQUIRE(bucket->GetEntryList()[0].record_count() == 1);
  REQUIRE(bucket->GetEntryList()[0].latest_record() == 100'000'123);
  REQUIRE(bucket->GetEntryList()[0].oldest_record() == 100'000'123);

  SECTION("error if not exist") {
    err = storage->GetBucket("X");
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

TEST_CASE("storage::Storage should remove a bucket", "[storage][bucket]") {
  auto data_path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = data_path});

  Error err = storage->CreateBucket("bucket", MakeDefaultBucketSettings());
  REQUIRE(err == Error::kOk);
  REQUIRE(fs::exists(data_path / "bucket"));

  err = storage->RemoveBucket("bucket");
  REQUIRE(err == Error::kOk);
  REQUIRE_FALSE(fs::exists(data_path / "bucket"));

  err = storage->GetBucket("bucket");
  REQUIRE(err == Error{.code = 404, .message = "Bucket 'bucket' is not found"});

  SECTION("error if bucket is not found") {
    err = storage->RemoveBucket("X");
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

TEST_CASE("storage::Storage should recover at start", "[storage][bucket]") {
  SECTION("broken bucket") {
    auto dir = BuildTmpDirectory();
    fs::create_directory(dir / "broker_bucket");
    REQUIRE(IStorage::Build({.data_path = dir}));
  }
}
