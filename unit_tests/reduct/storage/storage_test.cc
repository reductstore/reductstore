// Copyright 2021 Alexey Timin

#include "reduct/storage/storage.h"

#include <catch2/catch.hpp>

#include "reduct/config.h"
#include "reduct/helpers.h"

using reduct::api::ICreateBucketCallback;
using reduct::api::IInfoCallback;
using reduct::core::Error;
using reduct::storage::IStorage;

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  IInfoCallback::Response info;
  auto err = storage->OnInfo(&info, {});

  REQUIRE_FALSE(err);
  REQUIRE(info.version == reduct::kVersion);
}

TEST_CASE("storage::Storage should create a bucket", "[storage]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  ICreateBucketCallback::Request req{.name = "bucket"};
  auto err = storage->OnCreateBucket(nullptr, req);

  REQUIRE_FALSE(err);

  SECTION("error if already exists") {
    auto err = storage->OnCreateBucket(nullptr, req);
    REQUIRE(err == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("error if failed to create") {
    auto err = storage->OnCreateBucket(nullptr, {.name = ""});
    REQUIRE(err == Error{.code = 500, .message = "Internal error: Failed to create bucket"});
  }
}
