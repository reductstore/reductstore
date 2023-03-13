// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include <catch2/catch.hpp>

#include "reduct/auth/token_auth.h"
#include "reduct/auth/token_repository.h"
#include "reduct/helpers.h"

using reduct::auth::Anonymous;
using reduct::auth::Authenticated;
using reduct::auth::ITokenAuthorization;
using reduct::auth::ITokenRepository;
using reduct::core::Error;

TEST_CASE("auth::NoTokenRepository should validate any token and error") {
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory()});

  REQUIRE(repo->ValidateToken({"token-1"}) == Error::kOk);
  REQUIRE(repo->ValidateToken(Error::BadRequest("Any problems")) == Error::kOk);
}

TEST_CASE("auth::NoTokenRepository should return placeholder with full access") {
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory()});

  auto [token, err] = repo->ValidateToken({""});
  REQUIRE(err == Error::kOk);
  REQUIRE(token.name() == "AUTHENTICATION-DISABLED");
  REQUIRE(token.permissions().full_access());
}
