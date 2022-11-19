// Copyright 2022 Alexey Timin

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

TEST_CASE("TokenApi::Create should create a token and return its value") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(true);
  permissions.mutable_read()->Add("bucket-1");
  permissions.mutable_write()->Add("bucket-2");

  {
    auto [receiver, err] = TokenApi::CreateToken(repo.get(), "new-token");
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
    auto [receiver, err] = TokenApi::CreateToken(repo.get(), "new-token");
    REQUIRE(err == Error::kOk);

    auto [resp, resp_err] = receiver(PrintToJson(permissions), true);
    REQUIRE(resp_err == Error::Conflict("Token 'new-token' already exists"));
  }
}

TEST_CASE("TokenApi::ListTokens should list tokens") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  REQUIRE(repo->Create("token-1", {}) == Error::kOk);
  REQUIRE(repo->Create("token-2", {}) == Error::kOk);

  auto [receiver, err] = TokenApi::ListTokens(repo.get());
  REQUIRE(err == Error::kOk);

  auto [resp, resp_err] = receiver({}, true);
  REQUIRE(resp_err == Error::kOk);
  REQUIRE(resp.content_length == 144);

  TokenRepo proto_message;
  JsonStringToMessage(resp.SendData().result, &proto_message);

  REQUIRE(proto_message.tokens(0) == repo->List().result[0]);
}

TEST_CASE("TokenApi::GetToken should show a token") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(true);
  permissions.mutable_read()->Add("bucket-1");
  permissions.mutable_write()->Add("bucket-2");

  REQUIRE(repo->Create("token-1", permissions) == Error::kOk);

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
    auto [receiver, err] = TokenApi::GetToken(repo.get(), "XXXX");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver({}, true).error == Error::NotFound("Token 'XXXX' doesn't exist"));
  }
}

TEST_CASE("TokenApi::RemoveToken should delete a token") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  REQUIRE(repo->Create("token-1", {}).error == Error::kOk);

  {
    auto [receiver, err] = TokenApi::RemoveToken(repo.get(), "token-1");
    REQUIRE(err == Error::kOk);

    auto [_, resp_err] = receiver({}, true);
    REQUIRE(resp_err == Error::kOk);
  }

  SECTION("not found") {
    auto [receiver, err] = TokenApi::RemoveToken(repo.get(), "XXXX");
    REQUIRE(err == Error::kOk);
    REQUIRE(receiver({}, true).error == Error::NotFound("Token 'XXXX' doesn't exist"));
  }
}
