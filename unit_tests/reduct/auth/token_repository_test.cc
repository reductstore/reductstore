// Copyright 2022 Alexey Timin

#include "reduct/auth/token_repository.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::auth::ITokenRepository;
using reduct::core::Error;

TEST_CASE("auth::TokenRepository should create a token") {
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory()});

  ITokenRepository::TokenPermisssions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  auto [token, err] = repo->Create("token", std::move(permissions));
  REQUIRE(err == Error::kOk);

  SECTION("check value") {
    REQUIRE(token.starts_with("token-"));
    REQUIRE(token.size() == 70);
  }

  SECTION("check if it's added") {
    auto [token_list, _] = repo->List();
    REQUIRE(token_list.size() == 1);
    REQUIRE(token_list[0].name() == "token");
  }
}

TEST_CASE("auth::TokenRepository should list tokens") {
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory()});
  {
    auto [token_list, err] = repo->List();

    REQUIRE(err == Error::kOk);
    REQUIRE(token_list.empty());
  }

  ITokenRepository::TokenPermisssions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  REQUIRE(repo->Create("token-1", permissions) == Error::kOk);
  REQUIRE(repo->Create("token-2", permissions) == Error::kOk);

  {
    auto [token_list, err] = repo->List();
    REQUIRE(err == Error::kOk);
    REQUIRE(token_list.size() == 2);

    REQUIRE(token_list[0].name() == "token-1");
    REQUIRE(token_list[0].created_at().IsInitialized());
    REQUIRE(token_list[0].value().empty());
    REQUIRE_FALSE(token_list[0].has_permissions());

    REQUIRE(token_list[1].name() == "token-2");
    REQUIRE(token_list[1].created_at().IsInitialized());
    REQUIRE(token_list[1].value().empty());
    REQUIRE_FALSE(token_list[1].has_permissions());
  }
}
