// Copyright 2022 Alexey Timin
#include "reduct/auth/token_auth.h"

#include <catch2/catch.hpp>

#include "reduct/auth/token_repository.h"
#include "reduct/helpers.h"

using reduct::auth::Anonymous;
using reduct::auth::Authenticated;
using reduct::auth::ITokenAuthorization;
using reduct::core::Error;

TEST_CASE("auth::TokenAuthorization should return 401 if head is bad") {
  auto auth = ITokenAuthorization::Build("xxxxxxx");
  auto repo = reduct::auth::ITokenRepository::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(auth->Check("", *repo, Authenticated()) == Error::Unauthorized("No bearer token in request header"));
  REQUIRE(auth->Check("xxx", *repo, Authenticated()) == Error::Unauthorized("No bearer token in request header"));
  REQUIRE(auth->Check("Bearer AABBCC", *repo, Authenticated()) == Error::Unauthorized("Invalid token"));
}

TEST_CASE("auth::TokenAuthorization should use API token to check") {
  auto auth = ITokenAuthorization::Build("we have init api token");

  auto repo = reduct::auth::ITokenRepository::Build({.data_path = BuildTmpDirectory()});
  auto [token, err] = repo->CreateToken("token-1", {});
  REQUIRE(err == Error::kOk);

  REQUIRE(auth->Check("Bearer " + token.value(), *repo, Authenticated()) == Error::kOk);
}

TEST_CASE("auth::TokenAuthorization should allow an invalid token for anonymous access") {
  auto auth = ITokenAuthorization::Build("we have init api token");
  REQUIRE(auth->Check("Bearer invalid-token",
                      *reduct::auth::ITokenRepository::Build({.data_path = BuildTmpDirectory()}),
                      Anonymous()) == Error::kOk);
  REQUIRE(auth->Check("",
                      *reduct::auth::ITokenRepository::Build({.data_path = BuildTmpDirectory()}),
                      Anonymous()) == Error::kOk);
}
