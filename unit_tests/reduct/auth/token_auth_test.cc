// Copyright 2022 Alexey Timin
#include "reduct/auth/token_auth.h"

#include <catch2/catch.hpp>

using reduct::auth::ITokenAuthentication;
using reduct::core::Error;


TEST_CASE("auth::TokenAuthorization should return 401 if head is bad") {
  auto auth = ITokenAuthentication::Build("xxxxxxx");

  REQUIRE(auth->Check("") == Error{.code = 401, .message = "No bearer token in request header"});
  REQUIRE(auth->Check("xxx") == Error{.code = 401, .message = "No bearer token in request header"});
  REQUIRE(auth->Check("Bearer AABBCC") == Error{.code = 401, .message = "Invalid token"});
}

TEST_CASE("auth::TokenAuthorization should use API token to check") {
  auto auth = ITokenAuthentication::Build("some_token");
  REQUIRE(auth->Check("Bearer some_token") == Error::kOk);
}
