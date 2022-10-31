// Copyright 2022 Alexey Timin

#include "reduct/api/bucket_api.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using google::protobuf::util::JsonStringToMessage;
using reduct::api::BucketApi;
using reduct::core::Error;
using reduct::storage::IStorage;

TEST_CASE("BucketApi::CreateBucket should create a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});
  SECTION("default settings") {
    auto [resp, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(resp.input_call("", true) == Error::kOk);

    auto output = resp.output_call();
    REQUIRE(output.error == Error::kOk);
    REQUIRE(output.result.empty());

    REQUIRE(storage->GetBucket("bucket") == Error::kOk);
  }

  SECTION("with settings") {
    auto [resp, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(resp.input_call(R"({"quota_size": 100})", true) == Error::kOk);

    REQUIRE(storage->GetBucket("bucket").result.lock()->GetSettings().quota_size() == 100);
  }

  SECTION("already exists") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [resp, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(resp.input_call("", true) == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("bad syntax") {
    auto [resp, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(resp.input_call("{", true).code == 422);
  }
}

TEST_CASE("BucketApi::GetBucket should get a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [resp, err] = BucketApi::GetBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    reduct::proto::api::FullBucketInfo info;
    JsonStringToMessage(resp.output_call().result, &info);

    REQUIRE(info.info().name() == storage->GetBucket("bucket").result.lock()->GetInfo().name());
  }

  SECTION("doesn't exist") {
    auto [resp, err] = BucketApi::GetBucket(storage.get(), "bucket");
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }
}

TEST_CASE("BucketApi::HeadBucket should get a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [resp, err] = BucketApi::HeadBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);
    REQUIRE(resp.content_length == 0);
    REQUIRE(resp.output_call().result.empty());
  }

  SECTION("doesn't exist") {
    auto [resp, err] = BucketApi::HeadBucket(storage.get(), "bucket");
    REQUIRE(err == Error{.code = 404, .message = ""});
  }
}

TEST_CASE("BucketApi::UpdateBucket should update a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [resp, err] = BucketApi::UpdateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(resp.input_call(R"({"quota_size": 100})", true) == Error::kOk);

    REQUIRE(storage->GetBucket("bucket").result.lock()->GetSettings().quota_size() == 100);
  }

  SECTION("doesn't exist") {
    auto [resp, err] = BucketApi::UpdateBucket(storage.get(), "bucket");
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }

  SECTION("bad syntax") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [resp, err] = BucketApi::UpdateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(resp.input_call("{", true).code == 422);
  }
}

TEST_CASE("BucketApi::RemoveBucket should remove a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [resp, err] = BucketApi::RemoveBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);
    REQUIRE(resp.content_length == 0);
    REQUIRE(resp.output_call().result.empty());

    REQUIRE(storage->GetBucket("bucket") == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }

  SECTION("doesn't exist") {
    auto [resp, err] = BucketApi::RemoveBucket(storage.get(), "bucket");
    REQUIRE(err == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }
}
