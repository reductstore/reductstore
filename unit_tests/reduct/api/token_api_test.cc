// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/api/token_api.h"

#include <catch2/catch.hpp>
#include <google/protobuf/util/time_util.h>

#include "reduct/auth/token_repository.h"
#include "reduct/helpers.h"

using google::protobuf::util::JsonStringToMessage;
using google::protobuf::util::TimeUtil;

using reduct::api::PrintToJson;
using reduct::api::TokenApi;
using reduct::auth::ITokenRepository;
using reduct::core::Error;
using reduct::proto::api::Token;
using reduct::proto::api::TokenCreateResponse;
using reduct::proto::api::TokenRepo;
using Storage = reduct::storage::IStorage;  // fix windows build

TEST_CASE("TokenApi::CreateToken should create a token and return its value", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path, .api_token = "init-token"});
  auto storage = Storage::Build({.data_path = path});

  REQUIRE(storage->CreateBucket("bucket-1", {}) == Error::kOk);
  REQUIRE(storage->CreateBucket("bucket-2", {}) == Error::kOk);

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(true);
  permissions.mutable_read()->Add("bucket-1");
  permissions.mutable_write()->Add("bucket-2");

  {
    auto [receiver, err] = TokenApi::CreateToken(repo.get(), storage.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(PrintToJson(permissions), true);

    REQUIRE(resp_err == Error::kOk);

    TokenCreateResponse message;
    JsonStringToMessage(resp.SendData().result, &message);
    REQUIRE(message.value().starts_with("new-token-"));
    REQUIRE(message.created_at() >= TimeUtil::GetCurrentTime());

    REQUIRE(repo->FindByName("new-token").error == Error::kOk);
    REQUIRE(repo->FindByName("new-token").result.permissions().full_access());
  }

  SECTION("already exists") {
    auto [receiver, err] = TokenApi::CreateToken(repo.get(), storage.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(PrintToJson(permissions), true);
    REQUIRE(resp_err == Error::Conflict("Token 'new-token' already exists"));
  }

  SECTION("check if bucket exists") {
    permissions.mutable_read()->Add("bucket-3");
    auto [receiver, err] = TokenApi::CreateToken(repo.get(), storage.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(PrintToJson(permissions), true);
    REQUIRE(resp_err == Error::UnprocessableEntity("Bucket 'bucket-3' doesn't exist"));
  }
}

TEST_CASE("TokenApi::ListTokens should list tokens", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path, .api_token = "init-token"});

  REQUIRE(repo->CreateToken("token-1", {}) == Error::kOk);
  REQUIRE(repo->CreateToken("token-2", {}) == Error::kOk);

  auto [receiver, err] = TokenApi::ListTokens(repo.get());
  REQUIRE(err == Error::kOk);

  auto [resp, resp_err] = receiver({}, true);
  REQUIRE(resp_err == Error::kOk);
  REQUIRE(resp.content_length == 144);

  TokenRepo proto_message;
  JsonStringToMessage(resp.SendData().result, &proto_message);

  REQUIRE(proto_message.tokens(0) == repo->GetTokenList().result[0]);
}

TEST_CASE("TokenApi::GetToken should show a token", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path, .api_token = "init-token"});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(true);
  permissions.mutable_read()->Add("bucket-1");
  permissions.mutable_write()->Add("bucket-2");

  REQUIRE(repo->CreateToken("token-1", permissions) == Error::kOk);

  {
    auto [receiver, err] = TokenApi::GetToken(repo.get(), "token-1");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver({}, true);
    REQUIRE(resp_err == Error::kOk);
    REQUIRE(resp.content_length == 141);

    Token proto_message;
    JsonStringToMessage(resp.SendData().result, &proto_message);

    REQUIRE(proto_message == repo->FindByName("token-1").result);
  }

  SECTION("not found") {
    REQUIRE(TokenApi::GetToken(repo.get(), "XXXX").error == Error::NotFound("Token 'XXXX' doesn't exist"));
  }
}

TEST_CASE("TokenApi::RemoveToken should delete a token", "[api]") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path, .api_token = "init-token"});

  REQUIRE(repo->CreateToken("token-1", {}).error == Error::kOk);

  {
    auto [receiver, err] = TokenApi::RemoveToken(repo.get(), "token-1");
    REQUIRE(err == Error::kOk);

    auto [_, resp_err] = receiver({}, true);
    REQUIRE(resp_err == Error::kOk);
  }

  SECTION("not found") {
    REQUIRE(TokenApi::RemoveToken(repo.get(), "XXXX").error == Error::NotFound("Token 'XXXX' doesn't exist"));
  }
}
