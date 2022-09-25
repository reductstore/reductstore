// Copyright 2022 Alexey Timin
#include <catch2/catch.hpp>

#include "reduct/config.h"
#include "reduct/helpers.h"
#include "reduct/storage/storage.h"

using reduct::async::Task;
using reduct::core::Error;

using reduct::storage::IStorage;

#if 0
TEST_CASE("storage::Storage should provide info about itself", "[storage][server_api]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  std::this_thread::sleep_for(std::chrono::seconds(1));  // uptime 1 second

  auto [info, err] = storage->GetInfo();

  REQUIRE_FALSE(err);
  REQUIRE(info.version() == reduct::kVersion);
  REQUIRE(info.bucket_count() == 0);
  REQUIRE(info.usage() == 0);
  REQUIRE(info.uptime() >= 1);
  REQUIRE(info.defaults().bucket().max_block_size() == reduct::kDefaultMaxBlockSize);
  REQUIRE(info.defaults().bucket().quota_type() == reduct::proto::api::BucketSettings::NONE);
  REQUIRE(info.defaults().bucket().quota_size() == 0);
}

TEST_CASE("storage::Storage should be restored from filesystem", "[storage][server_api]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});

  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);
  auto ret =
      OnWriteEntry(storage.get(),
                   {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1000001", .content_length = "9"})
          .Get();
  REQUIRE(ret == Error::kOk);
  REQUIRE(ret.result->Write("some_blob") == Error::kOk);

  ret = OnWriteEntry(storage.get(),
                     {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "2000002", .content_length = "9"})
            .Get();
  REQUIRE(ret == Error::kOk);
  REQUIRE(ret.result->Write("some_blob") == Error::kOk);

  storage = IStorage::Build({.data_path = dir});

  auto [info, err] = storage->GetInfo();
  REQUIRE(info.bucket_count() == 1);
  REQUIRE(info.usage() == 18);
  REQUIRE(info.oldest_record() == 1000001);
  REQUIRE(info.latest_record() == 2000002);
}

TEST_CASE("storage::Storage should provide list of buckets", "[storage][server_api]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});
  REQUIRE(storage->CreateBucket("bucket_1", {}) == Error::kOk);
  REQUIRE(storage->CreateBucket("bucket_2", {}) == Error::kOk);

  REQUIRE(
      OnWriteEntry(storage.get(),
                   {.bucket_name = "bucket_1", .entry_name = "entry1", .timestamp = "1000001", .content_length = "9"})
          .Get()
          .result->Write("some_data") == Error::kOk);
  REQUIRE(
      OnWriteEntry(storage.get(),
                   {.bucket_name = "bucket_1", .entry_name = "entry2", .timestamp = "2000002", .content_length = "9"})
          .Get()
          .result->Write("some_data") == Error::kOk);
  REQUIRE(
      OnWriteEntry(storage.get(),
                   {.bucket_name = "bucket_2", .entry_name = "entry2", .timestamp = "3000003", .content_length = "9"})
          .Get()
          .result->Write("some_data") == Error::kOk);

  auto [resp, err] = storage->GetList();
  REQUIRE(resp.buckets_size() == 2);

  auto bucket = resp.buckets(0);
  REQUIRE(bucket.name() == "bucket_1");
  REQUIRE(bucket.size() == 18);
  REQUIRE(bucket.entry_count() == 2);
  REQUIRE(bucket.oldest_record() == 1'000'001);
  REQUIRE(bucket.latest_record() == 2'000'002);

  bucket = resp.buckets(1);
  REQUIRE(bucket.name() == "bucket_2");
  REQUIRE(bucket.size() == 9);
  REQUIRE(bucket.entry_count() == 1);
  REQUIRE(bucket.oldest_record() == 3'000'003);
  REQUIRE(bucket.latest_record() == 3'000'003);
}

#endif
