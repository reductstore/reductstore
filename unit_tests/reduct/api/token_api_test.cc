// Copyright 2022 Alexey Timin

#include "reduct/api/token_api.h"

#include <catch2/catch.hpp>

#include "reduct/auth/token_repository.h"
#include "reduct/helpers.h"

using reduct::api::TokenApi;
using reduct::auth::ITokenRepository;
using reduct::core::Error;
using reduct::api::PrintToJson;

TEST_CASE("TokenApi::Create should create a token and return its value") {
//  const auto path = BuildTmpDirectory();
//  auto repo = ITokenRepository::Build({.data_path = path});
//
//  ITokenRepository::TokenPermissions permissions;
//  permissions.set_full_access(true);
//  permissions.mutable_read()->Add("bucket-1");
//  permissions.mutable_write()->Add("bucket-2");
//
//  auto [resp, err] = TokenApi::CreateToken(repo.get(), "new-token");
//  REQUIRE(err == Error::kOk);
//
//  REQUIRE(resp.input_call(PrintToJson(permissions), true) == Error::kOk);
//  REQUIRE(resp.content_length == 12);
//  REQUIRE(resp.SendData().result == "ccc");
//
//  REQUIRE(repo->FindByName("new-token").error == Error::kOk);
//  REQUIRE(repo->FindByName("new-token").result.lock()->GetPermissions().full_access());
}
