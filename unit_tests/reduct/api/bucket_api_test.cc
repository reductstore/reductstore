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
    auto [receiver, err] = BucketApi::CreateBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    auto output = resp.SendData();
    REQUIRE(output.error == Error::kOk);
    REQUIRE(output.result.empty());

    REQUIRE(storage->GetBucket("bucket") == Error::kOk);
  }

  SECTION("with settings") {
    auto [receiver, err] = BucketApi::CreateBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver(R"({"quota_size": 100})", true) == Error::kOk);

    REQUIRE(storage->GetBucket("bucket").result.lock()->GetSettings().quota_size() == 100);
  }

  SECTION("already exists") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("", true) == Error{.code = 409, .message = "Bucket 'bucket' already exists"});
  }

  SECTION("bad syntax") {
    auto [receiver, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("{", true).error.code == 422);
  }
}

TEST_CASE("BucketApi::GetBucket should get a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::GetBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);

    reduct::proto::api::FullBucketInfo info;
    JsonStringToMessage(resp.SendData().result, &info);

    REQUIRE(info.info().name() == storage->GetBucket("bucket").result.lock()->GetInfo().name());
  }

  SECTION("doesn't exist") {
    auto [receiver, err] = BucketApi::GetBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    REQUIRE(receiver("", true).error == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }
}

TEST_CASE("BucketApi::HeadBucket should get a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::HeadBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);
    REQUIRE(resp.content_length == 0);
    REQUIRE(resp.SendData().result.empty());
  }

  SECTION("doesn't exist") {
    auto [receiver, err] = BucketApi::HeadBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    REQUIRE(receiver("", true).error == Error{.code = 404, .message = ""});
  }
}

TEST_CASE("BucketApi::UpdateBucket should update a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::UpdateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver(R"({"quota_size": 100})", true) == Error::kOk);

    REQUIRE(storage->GetBucket("bucket").result.lock()->GetSettings().quota_size() == 100);
  }

  SECTION("doesn't exist") {
    auto [receiver, err] = BucketApi::UpdateBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    REQUIRE(receiver("", true).error == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }

  SECTION("bad syntax") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::UpdateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("{", true).error.code == 422);
  }
}

TEST_CASE("BucketApi::RemoveBucket should remove a bucket") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::RemoveBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);
    REQUIRE(resp.content_length == 0);
    REQUIRE(resp.SendData().result.empty());

    REQUIRE(storage->GetBucket("bucket") == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }

  SECTION("doesn't exist") {
    auto [receiver, err] = BucketApi::RemoveBucket(storage.get(), "bucket");
    REQUIRE(err == Error::kOk);

    REQUIRE(receiver("", true) == Error{.code = 404, .message = "Bucket 'bucket' is not found"});
  }
}
