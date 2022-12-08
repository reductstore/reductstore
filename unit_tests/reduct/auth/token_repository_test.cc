// Copyright 2022 Alexey Timin

#include "reduct/auth/token_repository.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using reduct::auth::ITokenRepository;
using reduct::core::Error;

static std::unique_ptr<ITokenRepository> MakeRepo(std::filesystem::path path = BuildTmpDirectory()) {
  auto repo = ITokenRepository::Build({.data_path = path});
  {
    auto [token_list, err] = repo->GetTokenList();

    REQUIRE(err == Error::kOk);
    REQUIRE(token_list.empty());
  }

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  REQUIRE(repo->CreateToken("token-1", permissions) == Error::kOk);
  REQUIRE(repo->CreateToken("token-2", permissions) == Error::kOk);
  return repo;
}

TEST_CASE("auth::TokenRepository should create a token") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  auto [token, err] = repo->CreateToken("token", std::move(permissions));
  REQUIRE(err == Error::kOk);

  SECTION("check value") {
    REQUIRE(token.value().starts_with("token-"));
    REQUIRE(token.value().size() == 70);
  }

  SECTION("check if it's added") {
    auto [token_list, _] = repo->GetTokenList();
    REQUIRE(token_list.size() == 1);
    REQUIRE(token_list[0].name() == "token");
  }

  SECTION("name must be unique") {
    REQUIRE(repo->CreateToken("token", {}).error == Error::Conflict("Token 'token' already exists"));
  }

  SECTION("name can't be empty") {
    REQUIRE(repo->CreateToken("", {}).error == Error::UnprocessableEntity("Token name can't be empty"));
  }

  SECTION("must be persistent") {
    auto new_repo = ITokenRepository::Build({.data_path = path});
    auto [token_list, _] = new_repo->GetTokenList();
    REQUIRE(token_list.size() == 1);
    REQUIRE(token_list[0].name() == "token");
  }
}

TEST_CASE("auth::TokenRepository should update token") {
  const auto path = BuildTmpDirectory();
  auto repo = ITokenRepository::Build({.data_path = path});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");
  permissions.mutable_write()->Add("bucket_2");

  {
    auto [_, err] = repo->CreateToken("new-token", std::move(permissions));
    REQUIRE(err == Error::kOk);

    ITokenRepository::TokenPermissions new_permissions;
    new_permissions.set_full_access(true);

    REQUIRE(repo->UpdateToken("new-token", new_permissions) == Error::kOk);
  }

  SECTION("check if it's updated") {
    auto [token, _] = repo->FindByName("new-token");
    REQUIRE(token.permissions().full_access());
  }

  SECTION("must be persistent") {
    auto new_repo = ITokenRepository::Build({.data_path = path});
    auto [token, _] = new_repo->FindByName("new-token");
    REQUIRE(token.permissions().full_access());
  }

  SECTION("can't update non-existing token") {
    REQUIRE(repo->UpdateToken("non-existing-token", {}) == Error::NotFound("Token 'non-existing-token' doesn't exist"));
  }
}

TEST_CASE("auth::TokenRepository should list tokens") {
  auto repo = MakeRepo();

  auto [token_list, err] = repo->GetTokenList();
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
    REQUIRE(repo->FindByName("token-XXX").error == Error::NotFound("Token 'token-XXX' doesn't exist"));
  }
}

TEST_CASE("auth::TokenRepository should find s token by value") {
  auto repo = MakeRepo();

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(false);
  permissions.mutable_read()->Add("bucket_1");

  auto [token_resp, err] = repo->CreateToken("token-3", permissions);
  REQUIRE(err == Error::kOk);

  auto [token, find_err] = repo->ValidateToken(token_resp.value());
  REQUIRE(find_err == Error::kOk);
  REQUIRE(token.name() == "token-3");
  REQUIRE(token.created_at().IsInitialized());
  REQUIRE(token.permissions().read().at(0) == "bucket_1");
  REQUIRE(token.value().empty());

  SECTION("invalid token") {
    REQUIRE(repo->ValidateToken("WRONG_TOKEN").error == Error::Unauthorized("Invalid token"));
  }
}

TEST_CASE("auth::TokenRepository should remove token by name") {
  const auto path = BuildTmpDirectory();
  auto repo = MakeRepo(path);

  REQUIRE(repo->RemoveToken("token-1") == Error::kOk);
  REQUIRE(repo->FindByName("token-1").error.code == Error::kNotFound);

  SECTION("404 error") { REQUIRE(repo->RemoveToken("token-XXX") == Error::NotFound("Token 'token-XXX' doesn't exist")); }

  SECTION("should be persistent") {
    auto new_repo = ITokenRepository::Build({.data_path = path});
    REQUIRE(new_repo->FindByName("token-1").error.code == Error::kNotFound);
  }
}

TEST_CASE("auth::TokenRepository should create an init token") {
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory(), .api_token = "INIT_TOKEN"});
  auto [token, err] = repo->ValidateToken("INIT_TOKEN");

  REQUIRE(err == Error::kOk);
  REQUIRE(token.name() == "init-token");
}
