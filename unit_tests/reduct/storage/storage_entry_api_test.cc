// Copyright 2021-2022 Alexey Timin

#include <catch2/catch.hpp>

#include <filesystem>

#include "reduct/async/task.h"
#include "reduct/config.h"
#include "reduct/helpers.h"
#include "reduct/storage/storage.h"

using reduct::api::IListEntryCallback;
using reduct::api::IReadEntryCallback;
using reduct::api::IRemoveBucketCallback;
using reduct::api::IUpdateBucketCallback;
using reduct::api::IWriteEntryCallback;

using reduct::async::Run;
using reduct::async::Task;
using reduct::core::Error;

using reduct::proto::api::BucketSettings;
using reduct::storage::IStorage;

using reduct::MakeDefaultBucketSettings;
using reduct::OnCreateBucket;
using reduct::OnListEntry;
using reduct::OnReadEntry;
using reduct::OnWriteEntry;

namespace fs = std::filesystem;

TEST_CASE("storage::Storage should write and read data", "[storage][entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(
      OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = MakeDefaultBucketSettings()}).Get() ==
      Error::kOk);

  auto write_ret =
      OnWriteEntry(storage.get(),
                   {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .size = 9})
          .Get();

  REQUIRE(write_ret == Error::kOk);
  REQUIRE(write_ret.result->Write("some_data") == Error::kOk);

  auto [resp, err] =
      OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000"})
          .Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp.blob == "some_data");
  REQUIRE(resp.timestamp == "1610387457862000");

  SECTION("error if bucket is not found during writing") {
    Error error = OnWriteEntry(storage.get(), {.bucket_name = "X", .entry_name = "entry", .timestamp = "1000"}).Get();
    REQUIRE(error == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }

  SECTION("error if ts is empty or bad  during writing") {
    Error error = OnWriteEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = ""}).Get();
    REQUIRE(error == Error{.code = 422, .message = "'ts' parameter can't be empty"});

    error = OnWriteEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "XXXX"}).Get();
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

  REQUIRE(
      OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = MakeDefaultBucketSettings()}).Get() ==
      Error::kOk);
  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .size = 9})
              .Get()
              .result->Write("some_data") == Error::kOk);
  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862001", .size = 9})
              .Get()
              .result->Write("some_data") == Error::kOk);
  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862002", .size = 9})
              .Get()
              .result->Write("some_data") == Error::kOk);

  auto [result, err] = OnListEntry(storage.get(), {.bucket_name = "bucket",
                                                   .entry_name = "entry",
                                                   .start_timestamp = "1610387457862000",
                                                   .stop_timestamp = "1610387457862002"})
                           .Get();

  REQUIRE(err == Error::kOk);
  REQUIRE(result.records_size() == 2);
  REQUIRE(result.records(0).ts() == 1610387457862000);
  REQUIRE(result.records(0).size() == 9);
  REQUIRE(result.records(1).ts() == 1610387457862001);
  REQUIRE(result.records(1).size() == 9);

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
