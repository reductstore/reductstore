// Copyright 2022 Alexey Timin
#include "reduct/auth/token_auth.h"

#include <catch2/catch.hpp>

using reduct::api::IRefreshToken;
using reduct::async::Task;
using reduct::auth::ITokenAuthentication;
using reduct::core::Error;

inline Task<IRefreshToken::Result> OnRefreshToken(ITokenAuthentication* auth, IRefreshToken::Request req) {
  auto result = co_await auth->OnRefreshToken(req);
  co_return result;
}

TEST_CASE("auth::TokenAuthorization should return 401 if head is bad") {
  auto auth = ITokenAuthentication::Build("xxxxxxx");

  REQUIRE(auth->Check("") == Error{.code = 401, .message = "No bearer token in response header"});
  REQUIRE(auth->Check("xxx") == Error{.code = 401, .message = "No bearer token in response header"});
  REQUIRE(auth->Check("Bearer AABBCC") == Error{.code = 401, .message = "Invalid token"});
}

TEST_CASE("auth::TokenAuthorization should refresh token") {
  const std::string kApiToken = "sometoken";
  auto auth = ITokenAuthentication::Build(kApiToken, {.expiration_time_s = 1});

  auto [resp, err] = OnRefreshToken(auth.get(), kApiToken).Get();
  REQUIRE(err == Error::kOk);
  REQUIRE(auth->Check("Bearer " + resp.access_token()) == Error::kOk);

  SECTION("regenerate nonce") {
    auto new_auth = ITokenAuthentication::Build(kApiToken);
    REQUIRE(new_auth->Check("Bearer " + resp.access_token()) == Error{.code = 401, .message = "Invalid token"});
  }

  SECTION("check api token") {
    auto [_, refresh_err] = OnRefreshToken(auth.get(), "badtoken").Get();
    REQUIRE(refresh_err == Error{.code = 401, .message = "Invalid API token"});
  }

  SECTION("token expires") {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    REQUIRE(auth->Check("Bearer " + resp.access_token()) == Error{.code = 401, .message = "Expired token"});
  }
}
