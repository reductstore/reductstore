// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include <catch2/catch.hpp>

#include <thread>

#include "reduct/config.h"
#include "reduct/helpers.h"
#include "reduct/storage/storage.h"

using reduct::core::Error;

using reduct::MakeDefaultBucketSettings;
using reduct::core::Time;
using reduct::storage::IStorage;

using us = std::chrono::microseconds;

namespace fs = std::filesystem;

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

TEST_CASE("storage::Storage should be restored from filesystem", "[storage]]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});

  REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

  auto [bucket, bucket_err] = storage->GetBucket("bucket");

  auto entry = bucket.lock()->GetOrCreateEntry("entry").result.lock();
  REQUIRE(entry);

  REQUIRE(reduct::WriteOne(*entry, "some_blob", Time() + us(1000001)) == Error::kOk);
  REQUIRE(reduct::WriteOne(*entry, "some_blob", Time() + us(2000002)) == Error::kOk);

  storage = IStorage::Build({.data_path = dir});

  auto [info, err] = storage->GetInfo();
  REQUIRE(info.bucket_count() == 1);
  REQUIRE(info.usage() == 18);
  REQUIRE(info.oldest_record() == 1000001);
  REQUIRE(info.latest_record() == 2000002);
}

TEST_CASE("storage::Storage should provide list of buckets", "[storage]") {
  const auto dir = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = dir});
  REQUIRE(storage->CreateBucket("bucket_1", {}) == Error::kOk);
  REQUIRE(storage->CreateBucket("bucket_2", {}) == Error::kOk);

  {
    auto [bucket, bucket_err] = storage->GetBucket("bucket_1");

    auto entry = bucket.lock()->GetOrCreateEntry("entry1").result.lock();
    REQUIRE(entry);

    REQUIRE(reduct::WriteOne(*entry, "some_blob", Time() + us(1000001)) == Error::kOk);

    entry = bucket.lock()->GetOrCreateEntry("entry2").result.lock();
    REQUIRE(entry);
    REQUIRE(reduct::WriteOne(*entry, "some_blob", Time() + us(2000002)) == Error::kOk);
  }

  {
    auto [bucket, bucket_err] = storage->GetBucket("bucket_2");

    auto entry = bucket.lock()->GetOrCreateEntry("entry2").result.lock();
    REQUIRE(entry);

    REQUIRE(reduct::WriteOne(*entry, "some_blob", Time() + us(3000003)) == Error::kOk);
  }

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

TEST_CASE("storage::Storage should create a bucket", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  REQUIRE_FALSE(storage->CreateBucket("bucket", MakeDefaultBucketSettings()));

  SECTION("error if already exists") {
    auto err = storage->CreateBucket("bucket", MakeDefaultBucketSettings());
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    auto err = storage->CreateBucket("", MakeDefaultBucketSettings());
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }

  SECTION("wrong name") {
    auto err = storage->CreateBucket("bucket/sak#", MakeDefaultBucketSettings());
    REQUIRE(err == Error{.code = 422, .message = "Bucket name can contain only letters, digests and [-,_] symbols"});
  }
}

TEST_CASE("storage::Storage should get a bucket", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  const auto settings = MakeDefaultBucketSettings();

  REQUIRE_FALSE(storage->CreateBucket("bucket", MakeDefaultBucketSettings()));

  {
    auto [bucket_ptr, err] = storage->GetBucket("bucket");
    REQUIRE(err == Error::kOk);

    err = reduct::WriteOne(*bucket_ptr.lock()->GetOrCreateEntry("entry_1").result.lock(), "someblob",
                           reduct::core::Time() + std::chrono::microseconds(100000123));
    REQUIRE(err == Error::kOk);
  }

  auto [bucket_ptr, err] = storage->GetBucket("bucket");
  REQUIRE(err == Error::kOk);

  SECTION("error if not exist") {
    err = storage->GetBucket("X");
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}

TEST_CASE("storage::Storage should remove a bucket", "[storage]") {
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

TEST_CASE("storage::Storage should recover at start", "[storage]]") {
  SECTION("broken bucket") {
    auto dir = BuildTmpDirectory();
    fs::create_directory(dir / "broker_bucket");
    REQUIRE(IStorage::Build({.data_path = dir}));
  }
}
