// Copyright 2022 Alexey Timin
#include <catch2/catch.hpp>

#include "reduct/config.h"
#include "reduct/helpers.h"
#include "reduct/storage/storage.h"

using reduct::api::IInfoCallback;
using reduct::api::IListStorageCallback;

using reduct::async::Task;
using reduct::core::Error;

using reduct::storage::IStorage;

using reduct::OnCreateBucket;
using reduct::OnInfo;
using reduct::OnStorageList;
using reduct::OnWriteEntry;

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  std::this_thread::sleep_for(std::chrono::seconds(1));  // uptime 1 second

  auto task = OnInfo(storage.get());
  auto [resp, err] = task.Get();

  REQUIRE_FALSE(err);
  REQUIRE(resp.info.version() == reduct::kVersion);
  REQUIRE(resp.info.bucket_count() == 0);
  REQUIRE(resp.info.usage() == 0);
  REQUIRE(resp.info.uptime() >= 1);
}

TEST_CASE("storage::Storage should be restored from filesystem", "[storage][entry]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});

  REQUIRE(OnCreateBucket(storage.get(), {.bucket_name = "bucket", .bucket_settings = {}}).Get() == Error::kOk);
  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "1000000", .blob = "some_data"})
              .Get() == Error::kOk);

  REQUIRE(OnWriteEntry(storage.get(),
                       {.bucket_name = "bucket", .entry_name = "entry", .timestamp = "2000000", .blob = "some_data"})
              .Get() == Error::kOk);

  storage = IStorage::Build({.data_path = dir});

  auto [resp, err] = OnInfo(storage.get()).Get();
  REQUIRE(resp.info.bucket_count() == 1);
  REQUIRE(resp.info.usage() == 22);
  REQUIRE(resp.info.oldest_record() == 1);
  REQUIRE(resp.info.latest_record() == 2);
}