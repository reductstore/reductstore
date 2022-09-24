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
using reduct::OnGetBucket;
using reduct::OnRemoveBucket;
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

  auto [resp, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp.settings() == settings);

  REQUIRE(resp.info().name() == "bucket");
  REQUIRE(resp.info().size() == 8);
  REQUIRE(resp.info().entry_count() == 1);
  REQUIRE(resp.info().oldest_record() == 100'000'123);
  REQUIRE(resp.info().latest_record() == 100'000'123);

  REQUIRE(resp.entries_size() == 1);
  REQUIRE(resp.entries(0).name() == "entry_1");
  REQUIRE(resp.entries(0).size() == 8);
  REQUIRE(resp.entries(0).block_count() == 1);
  REQUIRE(resp.entries(0).record_count() == 1);
  REQUIRE(resp.entries(0).latest_record() == 100'000'123);
  REQUIRE(resp.entries(0).oldest_record() == 100'000'123);

  SECTION("error if not exist") {
    err = OnGetBucket(storage.get(), {.bucket_name = "X"}).Get();
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

TEST_CASE("storage::Storage should change settings of bucket", "[stoage]") {
  auto data_path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = data_path});

  REQUIRE(storage->CreateBucket("bucket", MakeDefaultBucketSettings()) == Error::kOk);

  SECTION("should update full settings") {
    BucketSettings settings;
    settings.set_max_block_size(10);
    settings.set_quota_type(BucketSettings::FIFO);
    settings.set_quota_size(1000);
    settings.set_max_block_records(500);

    IUpdateBucketCallback::Request change_req{
        .bucket_name = "bucket",
        .new_settings = settings,
    };

    REQUIRE(OnChangeBucketSettings(storage.get(), change_req).Get() == Error::kOk);
    auto [info, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
    REQUIRE(err == Error::kOk);
    REQUIRE(info.settings() == change_req.new_settings);
  }

  SECTION("should update part of settings") {
    BucketSettings settings;
    settings.set_quota_type(BucketSettings::FIFO);

    IUpdateBucketCallback::Request change_req{
        .bucket_name = "bucket",
        .new_settings = settings,
    };

    REQUIRE(OnChangeBucketSettings(storage.get(), change_req).Get() == Error::kOk);
    auto [info, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
    REQUIRE(err == Error::kOk);
    REQUIRE(info.settings().max_block_size() == MakeDefaultBucketSettings().max_block_size());
    REQUIRE(info.settings().quota_type() == change_req.new_settings.quota_type());
  }

  SECTION("settings update should fail if underlying directory has been deleted") {
    BucketSettings settings;
    settings.set_quota_size(100);

    IUpdateBucketCallback::Request change_req{.bucket_name = "bucket", .new_settings = settings};

    fs::remove_all(data_path / "bucket");
    auto result = OnChangeBucketSettings(storage.get(), change_req).Get();
    REQUIRE(result == Error{.code = 500, .message = "Failed to open file of bucket settings"});
  }
}

TEST_CASE("storage::Storage should remove a bucket", "[storage][bucket]") {
  auto data_path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = data_path});

  Error err = storage->CreateBucket("bucket", MakeDefaultBucketSettings());
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
