// Copyright 2021-2022 Alexey Timin

#include <catch2/catch.hpp>

#include <filesystem>

#include "reduct/async/task.h"
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
using reduct::OnGetBucket;
using reduct::OnListEntry;
using reduct::OnQuery;
using reduct::OnReadEntry;
using reduct::OnWriteEntry;

using reduct::async::IAsyncReader;

namespace fs = std::filesystem;

TEST_CASE("storage::Storage should write and read data", "[storage][entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(
      OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = MakeDefaultBucketSettings()}).Get() ==
      Error::kOk);

  auto write_ret =
      OnWriteEntry(
          storage.get(),
          {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .content_length = "9"})
          .Get();

  REQUIRE(write_ret == Error::kOk);
  REQUIRE(write_ret.result->Write("some_data") == Error::kOk);

  auto [resp, err] =
      OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000"})
          .Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(resp->Read().result == IAsyncReader::DataChunk{.data = "some_data", .last = true});

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

  SECTION("error if size is empty or bad  during writing") {
    Error error = OnWriteEntry(storage.get(),
                               {
                                   .bucket_name = "bucket",
                                   .entry_name = "entry",
                                   .timestamp = "100",
                                   .content_length = "",
                               })
                      .Get();
    REQUIRE(error == Error{.code = 411, .message = "bad or empty content-length"});

    error = OnWriteEntry(storage.get(),
                         {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "100", .content_length = "xxx"})
                .Get();
    REQUIRE(error == Error{.code = 411, .message = "bad or empty content-length"});

    error = OnWriteEntry(storage.get(),
                         {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "100", .content_length = "-100"})
                .Get();
    REQUIRE(error == Error{.code = 411, .message = "negative content-length"});
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

  SECTION("error if no entry to read") {
    Error error =
        OnReadEntry(storage.get(), {.bucket_name = "bucket", .entry_name = "NOTEXIT", .timestamp = "1000"}).Get();
    REQUIRE(error == Error{.code = 404, .message = "Entry 'NOTEXIT' could not be found"});

    auto bucket_info = OnGetBucket(storage.get(), {.bucket_name = "bucket"}).Get();
    REQUIRE(bucket_info == Error::kOk);
    REQUIRE(bucket_info.result.entries_size() == 1);  // we don't create a new one
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
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .content_length = "9"})
              .Get()
              .result->Write("some_data") == Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862001", .content_length = "9"})
              .Get()
              .result->Write("some_data") == Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862002", .content_length = "9"})
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

TEST_CASE("storage::Storage should query records by timestamps", "[storage][entry]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(
      OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = MakeDefaultBucketSettings()}).Get() ==
      Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862000", .content_length = "9"})
              .Get()
              .result->Write("some_data") == Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862001", .content_length = "9"})
              .Get()
              .result->Write("some_data") == Error::kOk);
  REQUIRE(OnWriteEntry(
              storage.get(),
              {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1610387457862002", .content_length = "9"})
              .Get()
              .result->Write("some_data") == Error::kOk);

  auto [result, err] = OnQuery(storage.get(), {.bucket_name = "bucket",
                                               .entry_name = "entry",
                                               .start_timestamp = "1610387457862000",
                                               .stop_timestamp = "1610387457862002"})
                           .Get();

  REQUIRE(err == Error::kOk);
  REQUIRE(result.id() >= 0);

  SECTION("error if bad timestamps") {
    Error bad_ts_err =
        OnQuery(storage.get(),
                {.bucket_name = "bucket", .entry_name = "entry", .start_timestamp = "XXX", .stop_timestamp = "1200"})
            .Get();
    REQUIRE(bad_ts_err ==
            Error{.code = 422,
                  .message = "Failed to parse 'start_timestamp' parameter: XXX should unix times in microseconds"});

    bad_ts_err =
        OnQuery(storage.get(),
                {.bucket_name = "bucket", .entry_name = "entry", .start_timestamp = "1000", .stop_timestamp = "XXX"})
            .Get();
    REQUIRE(bad_ts_err ==
            Error{.code = 422,
                  .message = "Failed to parse 'stop_timestamp' parameter: XXX should unix times in microseconds"});
  }

  SECTION("error if bad ttl") {
    Error bad_ts_err = OnQuery(storage.get(), {.bucket_name = "bucket", .entry_name = "entry", .ttl = "XXX"}).Get();
    REQUIRE(bad_ts_err ==
            Error{.code = 422,
                  .message = "Failed to parse 'ttl' parameter: XXX should be unsigned integer"});
  }

  SECTION("error, if bucket not found") {
    REQUIRE(OnQuery(storage.get(), {.bucket_name = "UNKNOWN_BUCKET",
                                    .entry_name = "entry",
                                    .start_timestamp = "1000",
                                    .stop_timestamp = "1200"})
                .Get()
                .error == Error{.code = 404, .message = "Bucket 'UNKNOWN_BUCKET' is not found"});
  }
}
