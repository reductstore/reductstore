// Copyright 2021 Alexey Timin

#include "reduct/storage/storage.h"

#include <catch2/catch.hpp>

#include "reduct/async/task.h"
#include "reduct/config.h"
#include "reduct/helpers.h"

using reduct::api::ICreateBucketCallback;
using reduct::api::IGetBucketCallback;
using reduct::api::IInfoCallback;
using reduct::async::Run;
using reduct::async::Task;
using reduct::core::Error;
using reduct::storage::IStorage;

Task<IInfoCallback::Result> OnInfo(IStorage& storage) {
  auto result = co_await storage.OnInfo({});
  co_return result;
}

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto task = OnInfo(*storage);
  auto [info, err] = task.Get();

  REQUIRE_FALSE(err);
  REQUIRE(info.version == reduct::kVersion);
}

Task<ICreateBucketCallback::Result> OnCreateBucket(IStorage& storage, ICreateBucketCallback::Request req) {
  auto result = co_await storage.OnCreateBucket(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should create a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.name = "bucket"};
  Error err = OnCreateBucket(*storage, req).Get();
  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    err = OnCreateBucket(*storage, req).Get();
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    err = OnCreateBucket(*storage, {.name = ""}).Get();
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}

Task<IGetBucketCallback::Result> OnGetBucket(IStorage& storage, IGetBucketCallback::Request req) {
  auto result = co_await storage.OnGetBucket(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should get a bucket", "[storage][bucket]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.name = "bucket"};
  Error err = OnCreateBucket(*storage, req).Get();
  REQUIRE_FALSE(err);

  err = OnGetBucket(*storage, {.name = "bucket"}).Get();
  REQUIRE_FALSE(err);

  SECTION("error if not exist") {
    err = OnGetBucket(*storage, {.name = "X"}).Get();
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'X' is not found"});
  }
}
