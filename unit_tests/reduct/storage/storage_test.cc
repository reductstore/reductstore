// Copyright 2021 Alexey Timin

#include "reduct/storage/storage.h"

#include <catch2/catch.hpp>

#include <filesystem>

#include "reduct/async/task.h"
#include "reduct/config.h"
#include "reduct/helpers.h"

using reduct::api::ICreateBucketCallback;
using reduct::api::IGetBucketCallback;
using reduct::api::IInfoCallback;
using reduct::api::IReadEntryCallback;
using reduct::api::IRemoveBucketCallback;
using reduct::api::IWriteEntryCallback;

using reduct::async::Run;
using reduct::async::Task;
using reduct::core::Error;
using reduct::storage::IStorage;

namespace fs = std::filesystem;

Task<IInfoCallback::Result> OnInfo(IStorage* storage) {
  auto result = co_await storage->OnInfo({});
  co_return result;
}

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto task = OnInfo(storage.get());
  auto [info, err] = task.Get();

  REQUIRE_FALSE(err);
  REQUIRE(info.version == reduct::kVersion);
}

Task<ICreateBucketCallback::Result> OnCreateBucket(IStorage* storage, ICreateBucketCallback::Request req) {
  auto result = co_await storage->OnCreateBucket(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should create a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.name = "bucket"};
  Error err = OnCreateBucket(storage.get(), req).Get();
  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    err = OnCreateBucket(storage.get(), req).Get();
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    err = OnCreateBucket(storage.get(), {.name = ""}).Get();
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}

Task<IGetBucketCallback::Result> OnGetBucket(IStorage* storage, IGetBucketCallback::Request req) {
  auto result = co_await storage->OnGetBucket(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should get a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.name = "bucket"};
  Error err = OnCreateBucket(storage.get(), req).Get();
  REQUIRE_FALSE(err);

  err = OnGetBucket(storage.get(), {.name = "bucket"}).Get();
  REQUIRE_FALSE(err);

  SECTION("error if not exist") {
    err = OnGetBucket(storage.get(), {.name = "X"}).Get();
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

Task<IRemoveBucketCallback::Result> OnRemoveBucket(IStorage* storage, IRemoveBucketCallback::Request req) {
  auto result = co_await storage->OnRemoveBucket(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should remove a bucket", "[storage][bucket]") {
  auto data_path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = data_path});

  ICreateBucketCallback::Request req{.name = "bucket"};
  Error err = OnCreateBucket(storage.get(), req).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(fs::exists(data_path / "bucket"));

  err = OnRemoveBucket(storage.get(), {.name = "bucket"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE_FALSE(fs::exists(data_path / "bucket"));

  err = OnGetBucket(storage.get(), {.name = "bucket"}).Get();
  REQUIRE(err == Error{.code = 404, .message = "Bucket 'bucket' is not found"});

  SECTION("error if bucket is not found") {
    err = OnRemoveBucket(storage.get(), {.name = "X"}).Get();
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

Task<IWriteEntryCallback::Result> OnWriteEntry(IStorage* storage, IWriteEntryCallback::Request req) {
  auto result = co_await storage->OnWriteEntry(std::move(req));
  co_return result;
}

Task<IReadEntryCallback::Result> OnReadEntry(IStorage* storage, IReadEntryCallback::Request req) {
  auto result = co_await storage->OnReadEntry(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should write and read data", "[storage][entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(OnCreateBucket(storage.get(), {.name = "bucket"}).Get() == Error::kOk);

  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1000", .blob = "some_data"})
              .Get() == Error::kOk);
  auto [resp, err] =
      OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1000"}).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp.blob == "some_data");
  REQUIRE(resp.timestamp == "1000");

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
    REQUIRE(error == Error{.code = 400, .message = "'ts' parameter can't be empty"});

    error =
        OnWriteEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "XXXX", .blob = ""})
            .Get();
    REQUIRE(error ==
            Error{.code = 400, .message = "Failed to parse 'ts' parameter: XXXX should unix times in microseconds"});
  }

  SECTION("error if bucket is not found during reading") {
    Error error = OnReadEntry(storage.get(), {.bucket_name = "X", .entry_name = "entry", .timestamp = "1000"}).Get();
    REQUIRE(error == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }

  SECTION("error if ts is empty or bad  during reading") {
    Error error = OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = ""}).Get();
    REQUIRE(error == Error{.code = 400, .message = "'ts' parameter can't be empty"});

    error = OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "XXXX"}).Get();
    REQUIRE(error ==
            Error{.code = 400, .message = "Failed to parse 'ts' parameter: XXXX should unix times in microseconds"});
  }

  SECTION("error if the record not found") {
    Error error = OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "10"}).Get();
    REQUIRE(error == Error{.code = 404, .message = "No records for this timestamp"});
  }
}

TEST_CASE("storage::Storage should be restored from filesystem", "[entry]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});

  REQUIRE(OnCreateBucket(storage.get(), {.name = "bucket"}).Get() == Error::kOk);
  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1000", .blob = "some_data"})
              .Get() == Error::kOk);

  storage = IStorage::Build({.data_path = dir});

  auto [info, err] = OnInfo(storage.get()).Get();
  REQUIRE(info.bucket_count == 1);
  REQUIRE(info.entry_count == 1);
}