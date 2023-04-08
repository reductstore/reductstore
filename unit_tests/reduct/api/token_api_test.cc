// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/api/token_api.h"

#include <catch2/catch.hpp>
#include <google/protobuf/util/time_util.h>
#include <nlohmann/json.hpp>

#include "reduct/helpers.h"

using google::protobuf::util::JsonStringToMessage;
using google::protobuf::util::TimeUtil;

using reduct::api::PrintToJson;
using reduct::api::TokenApi;
using reduct::core::Error;
using Storage = reduct::storage::IStorage;  // fix windows build
namespace rs = reduct::rust_part;

TEST_CASE("TokenApi::CreateToken should create a token and return its value", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = rs::new_token_repo(path.string(), "init-token");

  auto storage = Storage::Build({.data_path = path});

  REQUIRE(storage->CreateBucket("bucket-1", {}) == Error::kOk);
  REQUIRE(storage->CreateBucket("bucket-2", {}) == Error::kOk);

  auto permissions = rs::new_token_permissions(true, {"bucket-1"}, {"bucket-2"});

  {
    auto [receiver, err] = TokenApi::CreateToken(*repo, storage.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(rs::token_permissions_to_json(*permissions).data(), true);

    REQUIRE(resp_err == Error::kOk);

    auto token_resp = rs::new_token_create_response();
    rs::json_to_token_create_response(resp.SendData().result, *token_resp);

    auto json = nlohmann::json::parse(rs::token_create_response_to_json(*token_resp));

    REQUIRE(std::string(json["value"]).starts_with("new-token-"));
    //    REQUIRE(json.get<std::string>("created_at") >= TimeUtil::GetCurrentTime());

    auto token = rs::new_token();
    auto token_err = rs::token_repo_validate_token(*repo, json["value"].get<std::string>().data(), *token);
    REQUIRE(token_err->status() == 200);

    auto json_token = rs::token_to_json(*token);
    auto token_json = nlohmann::json::parse(json_token);
    REQUIRE(token_json["permissions"]["full_access"] == true);
  }

  SECTION("already exists") {
    auto [receiver, err] = TokenApi::CreateToken(*repo, storage.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(rs::token_permissions_to_json(*permissions).data(), true);
    REQUIRE(resp_err == Error::Conflict("Token 'new-token' already exists"));
  }

  SECTION("check if bucket exists") {
    auto permission = rs::new_token_permissions(true, {"bucket-1", "bucket-3"}, {"bucket-2"});
    auto [receiver, err] = TokenApi::CreateToken(*repo, storage.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(rs::token_permissions_to_json(*permissions).data(), true);
    REQUIRE(resp_err == Error::UnprocessableEntity("Bucket 'bucket-3' doesn't exist"));
  }
}

TEST_CASE("TokenApi::ListTokens should list tokens", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = rs::new_token_repo(path.string(), "init-token");

  auto token = rs::new_token_create_response();
  auto permissions = rs::new_token_permissions(true, {"bucket-1"}, {"bucket-2"});
  auto create_err = rs::token_repo_create_token(*repo, "token-1", *permissions, *token);

  REQUIRE(create_err->status() == 200);

  create_err = rs::token_repo_create_token(*repo, "token-2", *permissions, *token);
  REQUIRE(create_err->status() == 200);

  auto [receiver, err] = TokenApi::ListTokens(*repo);
  REQUIRE(err == Error::kOk);

  auto [resp, resp_err] = receiver({}, true);
  REQUIRE(resp_err == Error::kOk);
  REQUIRE(resp.content_length == 144);

  auto json = nlohmann::json::parse(resp.SendData().result);

  REQUIRE(json.size() == 2);
  REQUIRE(json[0]["name"] == "token-1");
  REQUIRE(json[1]["name"] == "token-2");
}

TEST_CASE("TokenApi::GetToken should show a token", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = rs::new_token_repo(path.string(), "init-token");

  auto permissions = rs::new_token_permissions(true, {"bucket-1"}, {"bucket-2"});
  auto token_resp = rs::new_token_create_response();
  auto token_err = rs::token_repo_create_token(*repo, "token-1", *permissions, *token_resp);

  REQUIRE(token_err->status() == 200);

  {
    auto [receiver, err] = TokenApi::GetToken(*repo, "token-1");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver({}, true);
    REQUIRE(resp_err == Error::kOk);
    REQUIRE(resp.content_length == 141);

    auto json = nlohmann::json::parse(resp.SendData().result);
    REQUIRE(json["name"] == "token-1");
    REQUIRE(json["permissions"]["full_access"] == true);
  }

  SECTION("not found") {
    REQUIRE(TokenApi::GetToken(*repo, "XXXX").error == Error::NotFound("Token 'XXXX' doesn't exist"));
  }
}

TEST_CASE("TokenApi::RemoveToken should delete a token", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = rs::new_token_repo(path.string(), "init-token");

  auto permissions = rs::new_token_permissions(true, {"bucket-1"}, {"bucket-2"});
  auto token_resp = rs::new_token_create_response();
  auto token_err = rs::token_repo_create_token(*repo, "token-1", *permissions, *token_resp);

  REQUIRE(token_err->status() == 200);
  {
    auto [receiver, err] = TokenApi::RemoveToken(*repo, "token-1");
    REQUIRE(err == Error::kOk);

    auto [_, resp_err] = receiver({}, true);
    REQUIRE(resp_err == Error::kOk);
  }

  SECTION("not found") {
    REQUIRE(TokenApi::RemoveToken(*repo, "XXXX").error == Error::NotFound("Token 'XXXX' doesn't exist"));
  }
}
