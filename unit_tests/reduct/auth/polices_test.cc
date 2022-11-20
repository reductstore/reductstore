// Copyright 2022 Alexey Timin
#include <catch2/catch.hpp>

#include "reduct/auth/token_auth.h"

using reduct::auth::Anonymous;
using reduct::auth::Authenticated;
using reduct::auth::FullAccess;
using reduct::auth::ReadAccess;
using reduct::auth::WriteAccess;
using reduct::core::Error;
using reduct::proto::api::Token;

TEST_CASE("Anonymous should work without token") {
  REQUIRE(Anonymous().Validate({{}, Error::Unauthorized()}) == Error::kOk);
}

TEST_CASE("Authenticated should work with any valid token") {
  REQUIRE(Authenticated().Validate(Error::Unauthorized("Some error")) == Error::Unauthorized("Some error"));
  REQUIRE(Authenticated().Validate(Error::kOk) == Error::kOk);
}

TEST_CASE("FullAccess should check full access flag of token") {
  REQUIRE(Authenticated().Validate(Error::Unauthorized()) == Error::Unauthorized());
  REQUIRE(Authenticated().Validate(Error::kOk) == Error::kOk);

  Token::Permissions permissions;
  permissions.set_full_access(true);
  REQUIRE(FullAccess().Validate(permissions) == Error::kOk);

  permissions.set_full_access(false);
  REQUIRE(FullAccess().Validate(permissions) == Error::Forbidden("Token doesn't have full access"));
}

TEST_CASE("ReadAccess should work for full access flag and certain bucket") {
  Token::Permissions permissions;
  permissions.set_full_access(true);
  REQUIRE(ReadAccess("bucket-1").Validate(permissions) == Error::kOk);

  permissions.set_full_access(false);
  REQUIRE(ReadAccess("bucket-1").Validate(permissions) ==
          Error::Forbidden("Token doesn't have read access to bucket 'bucket-1'"));

  permissions.add_read("bucket-1");
  REQUIRE(ReadAccess("bucket-1").Validate(permissions) == Error::kOk);
}

TEST_CASE("WriteAccess should work for full access flag and certain bucket") {
  Token::Permissions permissions;
  permissions.set_full_access(true);
  REQUIRE(WriteAccess("bucket-1").Validate(permissions) == Error::kOk);

  permissions.set_full_access(false);
  REQUIRE(WriteAccess("bucket-1").Validate(permissions) ==
          Error::Forbidden("Token doesn't have write access to bucket 'bucket-1'"));

  permissions.add_write("bucket-1");
  REQUIRE(WriteAccess("bucket-1").Validate(permissions) == Error::kOk);
}
