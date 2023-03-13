// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/auth/token_auth.h"

#include <catch2/catch.hpp>

#include "reduct/auth/token_repository.h"
#include "reduct/helpers.h"

using reduct::auth::Anonymous;
using reduct::auth::Authenticated;
using reduct::auth::ITokenAuthorization;
using reduct::auth::ITokenRepository;
using reduct::core::Error;

TEST_CASE("auth::TokenAuthorization should return 401 if head is bad", "[auth]") {
  auto auth = ITokenAuthorization::Build("xxxxxxx");
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory(), .api_token = "init-token"});

  REQUIRE(auth->Check("", *repo, Authenticated()) == Error::Unauthorized("No bearer token in request header"));
  REQUIRE(auth->Check("xxx", *repo, Authenticated()) == Error::Unauthorized("No bearer token in request header"));
  REQUIRE(auth->Check("Bearer AABBCC", *repo, Authenticated()) == Error::Unauthorized("Invalid token"));
}

TEST_CASE("auth::TokenAuthorization should use API token to check", "[auth]") {
  auto auth = ITokenAuthorization::Build("we have init api token");

  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory(), .api_token = "init-token"});
  auto [token, err] = repo->CreateToken("token-1", {});
  REQUIRE(err == Error::kOk);

  REQUIRE(auth->Check("Bearer " + token.value(), *repo, Authenticated()) == Error::kOk);
}

TEST_CASE("auth::TokenAuthorization should allow an invalid token for anonymous access", "[auth]") {
  auto auth = ITokenAuthorization::Build("we have init api token");
  REQUIRE(auth->Check("Bearer invalid-token",
                      *reduct::auth::ITokenRepository::Build({.data_path = BuildTmpDirectory()}),
                      Anonymous()) == Error::kOk);
  REQUIRE(auth->Check("", *reduct::auth::ITokenRepository::Build({.data_path = BuildTmpDirectory()}), Anonymous()) ==
          Error::kOk);
}
