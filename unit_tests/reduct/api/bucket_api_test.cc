// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/api/bucket_api.h"

#include <catch2/catch.hpp>
#include <nlohmann/json.hpp>

#include "reduct/helpers.h"
#include "rust_part.h"

using google::protobuf::util::JsonStringToMessage;
using reduct::api::BucketApi;
using reduct::core::Error;
using reduct::storage::IStorage;
namespace rs = reduct::rust_part;

TEST_CASE("BucketApi::CreateBucket should create a bucket", "[api]") {
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
    REQUIRE(receiver("", true) == Error::Conflict("Bucket 'bucket' already exists"));
  }

  SECTION("bad syntax") {
    auto [receiver, err] = BucketApi::CreateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("{", true).error.code == Error::kUnprocessableEntity);
  }
}

TEST_CASE("BucketApi::GetBucket should get a bucket", "[api]") {
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
    REQUIRE(BucketApi::GetBucket(storage.get(), "bucket").error == Error::NotFound("Bucket 'bucket' is not found"));
  }
}

TEST_CASE("BucketApi::HeadBucket should get a bucket", "[api]") {
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
    REQUIRE(BucketApi::HeadBucket(storage.get(), "bucket").error == Error::NotFound("Bucket 'bucket' is not found"));
  }
}

TEST_CASE("BucketApi::UpdateBucket should update a bucket", "[api]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::UpdateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver(R"({"quota_size": 100})", true) == Error::kOk);

    REQUIRE(storage->GetBucket("bucket").result.lock()->GetSettings().quota_size() == 100);
  }

  SECTION("doesn't exist") {
    REQUIRE(BucketApi::UpdateBucket(storage.get(), "bucket").error == Error::NotFound("Bucket 'bucket' is not found"));
  }

  SECTION("bad syntax") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto [receiver, err] = BucketApi::UpdateBucket(storage.get(), "bucket");

    REQUIRE(err == Error::kOk);
    REQUIRE(receiver("{", true).error.code == Error::kUnprocessableEntity);
  }
}

TEST_CASE("BucketApi::RemoveBucket should remove a bucket", "[api]") {
  const auto path = BuildTmpDirectory();
  auto storage = IStorage::Build({.data_path = path});
  auto repo = rs::new_token_repo(path.string().data(), "init-token");

  SECTION("ok") {
    REQUIRE(storage->CreateBucket("bucket", {}) == Error::kOk);

    auto permissions = rs::new_token_permissions(false, {"bucket", "bucket-2"}, {"bucket", "bucket-3"});

    {
      auto resp = rs::new_token_create_response();
      auto err = reduct::rust_part::token_repo_create_token(*repo, "token", *permissions, *resp);
      REQUIRE(err->status() == 200);
    }

    auto [receiver, err] = BucketApi::RemoveBucket(storage.get(), *repo, "bucket");
    REQUIRE(err == Error::kOk);

    auto [resp, recv_err] = receiver("", true);
    REQUIRE(recv_err == Error::kOk);
    REQUIRE(resp.content_length == 0);
    REQUIRE(resp.SendData().result.empty());

    REQUIRE(storage->GetBucket("bucket") == Error::NotFound("Bucket 'bucket' is not found"));

    SECTION("remove from tokens") {
      auto token = rs::new_token();
      auto token_err = rs::token_repo_get_token(*repo, "token", *token);

        REQUIRE(token_err->status() == 200);
        auto json_token = nlohmann::json::parse(rs::token_to_json(*token));

        REQUIRE(json_token["permissions"]["read"].size() == 1);
        REQUIRE(json_token["permissions"]["read"][0] == "bucket-2");
        REQUIRE(json_token["permissions"]["write"].size() == 1);
        REQUIRE(json_token["permissions"]["write"][0] == "bucket-3");
    }
  }

  SECTION("doesn't exist") {
    REQUIRE(BucketApi::RemoveBucket(storage.get(), *repo, "bucket").error ==
            Error::NotFound("Bucket 'bucket' is not found"));
  }
}
