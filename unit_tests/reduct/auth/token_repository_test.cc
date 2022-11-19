// Copyright 2022 Alexey Timin

#include "reduct/auth/token_repository.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::auth::ITokenRepository;
using reduct::core::Error;

static std::unique_ptr<ITokenRepository> MakeRepo(std::filesystem::path path = BuildTmpDirectory()) {
  auto repo = ITokenRepository::Build({.data_path = path});
  {
    auto [token_list, err] = repo->List();

    REQUIRE(err == Error::kOk);
    REQUIRE(token_list.empty());
  }

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  REQUIRE(repo->Create("token-1", permissions) == Error::kOk);
  REQUIRE(repo->Create("token-2", permissions) == Error::kOk);
  return repo;
}

TEST_CASE("auth::TokenRepository should create a token") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  auto [token, err] = repo->Create("token", std::move(permissions));
  REQUIRE(err == Error::kOk);

  SECTION("check value") {
    REQUIRE(token.value().starts_with("token-"));
    REQUIRE(token.value().size() == 70);
  }

  SECTION("check if it's added") {
    auto [token_list, _] = repo->List();
    REQUIRE(token_list.size() == 1);
    REQUIRE(token_list[0].name() == "token");
  }

  SECTION("name must be unique") {
    REQUIRE(repo->Create("token", {}).error == Error{.code = 409, .message = "Token 'token' already exists"});
  }

  SECTION("name can't be empty") {
    REQUIRE(repo->Create("", {}).error == Error{.code = 422, .message = "Token name can't be empty"});
  }

  SECTION("must be persistent") {
    auto new_repo = ITokenRepository::Build({.data_path = path});
    auto [token_list, _] = new_repo->List();
    REQUIRE(token_list.size() == 1);
    REQUIRE(token_list[0].name() == "token");
  }
}

TEST_CASE("auth::TokenRepository should list tokens") {
  auto repo = MakeRepo();

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

TEST_CASE("auth::TokenRepository should find s token by name") {
  auto repo = MakeRepo();

  auto [token, error] = repo->FindByName("token-1");
  REQUIRE(error == Error::kOk);
  REQUIRE(token.name() == "token-1");
  REQUIRE(token.value().empty());
  REQUIRE_FALSE(token.permissions().full_access());
  REQUIRE(token.permissions().read().at(0) == "bucket_1");
  REQUIRE(token.permissions().write().at(0) == "bucket_2");

  SECTION("404 error") {
    REQUIRE(repo->FindByName("token-XXX").error == Error{.code = 404, .message = "Token 'token-XXX' doesn't exist"});
  }
}

TEST_CASE("auth::TokenRepository should find s token by value") {
  auto repo = MakeRepo();

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");

  auto [token_resp, err] = repo->Create("token-3", permissions);
  REQUIRE(err == Error::kOk);

  auto [token, find_err] = repo->FindByValue(token_resp.value());
  REQUIRE(find_err == Error::kOk);
  REQUIRE(token.name() == "token-3");
  REQUIRE(token.created_at().IsInitialized());
  REQUIRE(token.permissions().read().at(0) == "bucket_1");
  REQUIRE(token.value().empty());

  SECTION("404 error") {
    REQUIRE(repo->FindByValue("WRONG_TOKEN").error == Error{.code = 404, .message = "Wrong token value"});
  }
}

TEST_CASE("auth::TokenRepository should remove token by name") {
  const auto path = BuildTmpDirectory();
  auto repo = MakeRepo(path);

  REQUIRE(repo->Remove("token-1") == Error::kOk);
  REQUIRE(repo->FindByName("token-1").error.code == 404);

  SECTION("404 error") {
    REQUIRE(repo->Remove("token-XXX") == Error{.code = 404, .message = "Token 'token-XXX' doesn't exist"});
  }

  SECTION("should be persistent") {
    auto new_repo = ITokenRepository::Build({.data_path = path});
    REQUIRE(new_repo->FindByName("token-1").error.code == 404);
  }
}
