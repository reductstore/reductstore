// Copyright 2021 Alexey Timin

#include "reduct/storage/storage.h"

#include <catch2/catch.hpp>

#include "reduct/async/task.h"
#include "reduct/config.h"
#include "reduct/helpers.h"

using reduct::api::ICreateBucketCallback;
using reduct::api::IInfoCallback;
using reduct::async::Run;
using reduct::async::Task;
using reduct::core::Error;
using reduct::storage::IStorage;

Task<IInfoCallback::Result> OnInfo(IStorage& storage) {
  auto result = co_await storage.OnInfo({});
  co_return result;
}

Task<ICreateBucketCallback::Result> OnCreateBucket(IStorage& storage, ICreateBucketCallback::Request req) {
  auto result = co_await storage.OnCreateBucket(std::move(req));
  co_return result;
}

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto task = OnInfo(*storage);
  auto [info, err] = task.Get();

  REQUIRE_FALSE(err);
  REQUIRE(info.version == reduct::kVersion);
}

TEST_CASE("storage::Storage should create a bucket", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.name = "bucket"};
  auto [_, err] = OnCreateBucket(*storage, req).Get();
  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    auto [_, err1] = OnCreateBucket(*storage, req).Get();
    REQUIRE(err1 == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    auto [_, err1] = OnCreateBucket(*storage, {.name = ""}).Get();
    REQUIRE(err1 == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}
