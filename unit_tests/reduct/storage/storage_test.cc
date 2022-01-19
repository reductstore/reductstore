// Copyright 2021-2022 Alexey Timin

#include "reduct/storage/storage.h"

#include <catch2/catch.hpp>

#include <filesystem>

#include "reduct/async/task.h"
#include "reduct/config.h"
#include "reduct/helpers.h"

using reduct::api::ICreateBucketCallback;
using reduct::api::IGetBucketCallback;
using reduct::api::IInfoCallback;
using reduct::api::IListEntryCallback;
using reduct::api::IReadEntryCallback;
using reduct::api::IRemoveBucketCallback;
using reduct::api::IUpdateBucketCallback;
using reduct::api::IWriteEntryCallback;

using reduct::async::Run;
using reduct::async::Task;
using reduct::core::Error;
using reduct::storage::IStorage;

namespace fs = std::filesystem;

static const reduct::api::BucketSettings kDefaultBucketSettings = {
    .max_block_size = 1000, .quota_type = "NONE", .quota_size = 10};

Task<IInfoCallback::Result> OnInfo(IStorage* storage) {
  auto result = co_await storage->OnInfo({});
  co_return result;
}

Task<ICreateBucketCallback::Result> OnCreateBucket(IStorage* storage, ICreateBucketCallback::Request req) {
  auto result = co_await storage->OnCreateBucket(std::move(req));
  co_return result;
}

Task<IGetBucketCallback::Result> OnGetBucket(IStorage* storage, IGetBucketCallback::Request req) {
  auto result = co_await storage->OnGetBucket(std::move(req));
  co_return result;
}

Task<IUpdateBucketCallback::Result> OnChangeBucketSettings(IStorage* storage, IUpdateBucketCallback::Request req) {
  auto result = co_await storage->OnUpdateCallback(std::move(req));
  co_return result;
}

Task<IRemoveBucketCallback::Result> OnRemoveBucket(IStorage* storage, IRemoveBucketCallback::Request req) {
  auto result = co_await storage->OnRemoveBucket(std::move(req));
  co_return result;
}

Task<IWriteEntryCallback::Result> OnWriteEntry(IStorage* storage, IWriteEntryCallback::Request req) {
  auto result = co_await storage->OnWriteEntry(std::move(req));
  co_return result;
}

Task<IReadEntryCallback::Result> OnReadEntry(IStorage* storage, IReadEntryCallback::Request req) {
  auto result = co_await storage->OnReadEntry(std::move(req));
  co_return result;
}

Task<IListEntryCallback::Result> OnListEntry(IStorage* storage, IListEntryCallback::Request req) {
  auto result = co_await storage->OnListEntry(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should recover at start", "[storage][bucket]") {
  SECTION("broken bucket") {
    auto dir = BuildTmpDirectory();
    fs::create_directory(dir / "broker_bucket");
    REQUIRE(IStorage::Build({.data_path = dir}));
  }
}

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto task = OnInfo(storage.get());
  auto [info, err] = task.Get();

  REQUIRE_FALSE(err);
  REQUIRE(info.version == reduct::kVersion);
}

TEST_CASE("storage::Storage should create a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.bucket_name = "bucket", .bucket_settings = kDefaultBucketSettings};
  Error err = OnCreateBucket(storage.get(), req).Get();
  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    err = OnCreateBucket(storage.get(), req).Get();
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    err = OnCreateBucket(storage.get(), {.bucket_name = "", .bucket_settings = kDefaultBucketSettings}).Get();
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}

TEST_CASE("storage::Storage should get a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.bucket_name = "bucket", .bucket_settings = kDefaultBucketSettings};
  REQUIRE(OnCreateBucket(storage.get(), req).Get() == Error::kOk);

  auto [resp, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp.bucket_settings.max_block_size == kDefaultBucketSettings.max_block_size);
  REQUIRE(resp.bucket_settings.quota_type == kDefaultBucketSettings.quota_type);
  REQUIRE(resp.bucket_settings.quota_size == kDefaultBucketSettings.quota_size);

  SECTION("error if not exist") {
    err = OnGetBucket(storage.get(), {.bucket_name = "X"}).Get();
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

TEST_CASE("storage::Storage should change settings of bucket", "[entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{
      .bucket_name = "bucket",
      .bucket_settings = kDefaultBucketSettings,
  };
  REQUIRE(OnCreateBucket(storage.get(), req).Get() == Error::kOk);

  IUpdateBucketCallback::Request change_req{
      .bucket_name = "bucket", .new_settings = {.max_block_size = 10, .quota_type = "FIFO", .quota_size = 1000}};
  REQUIRE(OnChangeBucketSettings(storage.get(), change_req).Get() == Error::kOk);

  auto [info, err] = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(info.bucket_settings.max_block_size == change_req.new_settings.max_block_size);
  REQUIRE(info.bucket_settings.quota_type == change_req.new_settings.quota_type);
  REQUIRE(info.bucket_settings.quota_size == change_req.new_settings.quota_size);
}

TEST_CASE("storage::Storage should remove a bucket", "[storage][bucket]") {
  auto data_path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = data_path});

  ICreateBucketCallback::Request req{.bucket_name = "bucket", .bucket_settings = kDefaultBucketSettings};
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

TEST_CASE("storage::Storage should write and read data", "[storage][entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = kDefaultBucketSettings}).Get() ==
          Error::kOk);

  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .blob = "some_data"})
              .Get() == Error::kOk);
  auto [resp, err] =
      OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000"})
          .Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp.blob == "some_data");
  REQUIRE(resp.timestamp == "1610387457862000");

  SECTION("error if bucket is not found during writing") {
    Error error = OnWriteEntry(storage.get(),
                               {.bucket_name = "X", .entry_name = "entry", .timestamp = "1000", .blob = "some_data"})
                      .Get();
    REQUIRE(error == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }

  SECTION("error if ts is empty or bad  during writing") {
    Error error =
        OnWriteEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "", .blob = ""})
            .Get();
    REQUIRE(error == Error{.code = 422, .message = "'ts' parameter can't be empty"});

    error =
        OnWriteEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "XXXX", .blob = ""})
            .Get();
    REQUIRE(error ==
            Error{.code = 422, .message = "Failed to parse 'ts' parameter: XXXX should unix times in microseconds"});
  }

  SECTION("error if bucket is not found during reading") {
    Error error = OnReadEntry(storage.get(), {.bucket_name = "X", .entry_name = "entry", .timestamp = "1000"}).Get();
    REQUIRE(error == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }

  SECTION("error if ts is empty or bad  during reading") {
    Error error = OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = ""}).Get();
    REQUIRE(error == Error{.code = 422, .message = "'ts' parameter can't be empty"});

    error = OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "XXXX"}).Get();
    REQUIRE(error ==
            Error{.code = 422, .message = "Failed to parse 'ts' parameter: XXXX should unix times in microseconds"});
  }

  SECTION("error if the record not found") {
    Error error = OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "10"}).Get();
    REQUIRE(error == Error{.code = 404, .message = "No records for this timestamp"});
  }
}

TEST_CASE("storage::Storage should list records by timestamps", "[storage][entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = kDefaultBucketSettings}).Get() ==
          Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .blob = "some_data"})
              .Get() == Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862001", .blob = "some_data"})
              .Get() == Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862002", .blob = "some_data"})
              .Get() == Error::kOk);

  auto [result, err] = OnListEntry(storage.get(), {.bucket_name = "bucket",
                                                   .entry_name = "entry",
                                                   .start_timestamp = "1610387457862000",
                                                   .stop_timestamp = "1610387457862002"})
                           .Get();

  REQUIRE(err == Error::kOk);
  REQUIRE(result.records.size() == 2);
  REQUIRE(result.records[0].timestamp == 1610387457862000);
  REQUIRE(result.records[0].size == 11);
  REQUIRE(result.records[1].timestamp == 1610387457862001);
  REQUIRE(result.records[1].size == 11);

  SECTION("error if bad timestamps") {
    Error bad_ts_err =
        OnListEntry(
            storage.get(),
            {.bucket_name = "bucket", .entry_name = "entry", .start_timestamp = "XXX", .stop_timestamp = "1200"})
            .Get();
    REQUIRE(bad_ts_err ==
            Error{.code = 422,
                  .message = "Failed to parse 'start_timestamp' parameter: XXX should unix times in microseconds"});

    bad_ts_err =
        OnListEntry(
            storage.get(),
            {.bucket_name = "bucket", .entry_name = "entry", .start_timestamp = "1000", .stop_timestamp = "XXX"})
            .Get();
    REQUIRE(bad_ts_err ==
            Error{.code = 422,
                  .message = "Failed to parse 'stop_timestamp' parameter: XXX should unix times in microseconds"});
  }

  SECTION("error, if bucket not found") {
    REQUIRE(OnListEntry(storage.get(), {.bucket_name = "UNKNOWN_BUCKET",
                                        .entry_name = "entry",
                                        .start_timestamp = "1000",
                                        .stop_timestamp = "1200"})
                .Get()
                .error == Error{.code = 404, .message = "Bucket 'UNKNOWN_BUCKET' is not found"});
  }
}

TEST_CASE("storage::Storage should be restored from filesystem", "[storage][entry]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});

  REQUIRE(OnCreateBucket(storage.get(),
                         {.bucket_name = "bucket",
                          .bucket_settings = {.max_block_size = 1000, .quota_type = "NONE", .quota_size = 0}})
              .Get() == Error::kOk);
  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1000", .blob = "some_data"})
              .Get() == Error::kOk);

  storage = IStorage::Build({.data_path = dir});

  auto [info, err] = OnInfo(storage.get()).Get();
  REQUIRE(info.bucket_count == 1);
  REQUIRE(info.entry_count == 1);
}
