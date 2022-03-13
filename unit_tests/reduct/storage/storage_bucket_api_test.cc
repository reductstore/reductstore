// Copyright 2022 Alexey Timin

#include <catch2/catch.hpp>

#include <filesystem>

#include "reduct/async/task.h"
#include "reduct/helpers.h"
#include "reduct/storage/storage.h"

using reduct::async::Task;
using reduct::core::Error;

using reduct::api::ICreateBucketCallback;
using reduct::api::IGetBucketCallback;
using reduct::api::IUpdateBucketCallback;

using reduct::proto::api::BucketSettings;

using reduct::OnChangeBucketSettings;
using reduct::OnCreateBucket;
using reduct::OnGetBucket;
using reduct::OnRemoveBucket;
using reduct::OnWriteEntry;

using reduct::MakeDefaultBucketSettings;
using reduct::storage::IStorage;

namespace fs = std::filesystem;

TEST_CASE("storage::Storage should create a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.bucket_name = "bucket", .bucket_settings = MakeDefaultBucketSettings()};
  Error err = OnCreateBucket(storage.get(), req).Get();
  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    err = OnCreateBucket(storage.get(), req).Get();
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    err = OnCreateBucket(storage.get(), {.bucket_name = "", .bucket_settings = MakeDefaultBucketSettings()}).Get();
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}

TEST_CASE("storage::Storage should get a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  const auto settings = MakeDefaultBucketSettings();
  ICreateBucketCallback::Request req{.bucket_name = "bucket", .bucket_settings = settings};
  REQUIRE(OnCreateBucket(storage.get(), req).Get() == Error::kOk);

  REQUIRE(OnWriteEntry(storage.get(),
                       {
                           .bucket_name = "bucket",
                           .entry_name = "entry_1",
                           .timestamp = "100000000",
                           .blob = "somedata",
                       })
              .Get() == Error::kOk);
  auto [resp, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp.settings() == settings);

  REQUIRE(resp.info().name() == "bucket");
  REQUIRE(resp.info().size() == 10);
  REQUIRE(resp.info().entry_count() == 1);
  REQUIRE(resp.info().oldest_record() == 100);
  REQUIRE(resp.info().latest_record() == 100);

  REQUIRE(resp.entries_size() == 1);
  REQUIRE(resp.entries(0) == "entry_1");

  SECTION("error if not exist") {
    err = OnGetBucket(storage.get(), {.bucket_name = "X"}).Get();
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

TEST_CASE("storage::Storage should change settings of bucket", "[stoage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{
      .bucket_name = "bucket",
      .bucket_settings = MakeDefaultBucketSettings(),
  };
  REQUIRE(OnCreateBucket(storage.get(), req).Get() == Error::kOk);

  BucketSettings settings;
  settings.set_max_block_size(10);
  settings.set_quota_type(BucketSettings::FIFO);
  settings.set_quota_size(1000);
  IUpdateBucketCallback::Request change_req{
      .bucket_name = "bucket",
      .new_settings = settings,
  };
  REQUIRE(OnChangeBucketSettings(storage.get(), change_req).Get() == Error::kOk);

  auto [info, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(info.settings() == change_req.new_settings);
}

TEST_CASE("storage::Storage should remove a bucket", "[storage][bucket]") {
  auto data_path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = data_path});

  ICreateBucketCallback::Request req{.bucket_name = "bucket", .bucket_settings = MakeDefaultBucketSettings()};
  Error err = OnCreateBucket(storage.get(), req).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(fs::exists(data_path / "bucket"));

  err = OnRemoveBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE_FALSE(fs::exists(data_path / "bucket"));

  err = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error{.code = 404, .message = "Bucket 'bucket' is not found"});

  SECTION("error if bucket is not found") {
    err = OnRemoveBucket(storage.get(), {.bucket_name = "X"}).Get();
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
