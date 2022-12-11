// Copyright 2022 Alexey Timin

#include "reduct/api/server_api.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using google::protobuf::util::JsonStringToMessage;
using reduct::api::ServerApi;
using reduct::auth::ITokenRepository;
using reduct::core::Error;
using reduct::storage::IStorage;

TEST_CASE("ServerApi::Alive should return empty body") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [receiver, err] = ServerApi::Alive(storage.get());
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();
  REQUIRE(output.error == Error::kOk);
  REQUIRE(output.result.empty());
}

TEST_CASE("ServerApi::Info should return JSON") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [receiver, err] = ServerApi::Info(storage.get());
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();
  REQUIRE(output.error == Error::kOk);

  reduct::proto::api::ServerInfo info;
  JsonStringToMessage(output.result, &info);
  REQUIRE(info.version() == storage->GetInfo().result.version());
}

TEST_CASE("ServerApi::List should return JSON") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [receiver, err] = ServerApi::List(storage.get());
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();

  REQUIRE(output.error == Error::kOk);

  reduct::proto::api::BucketInfoList list;
  JsonStringToMessage(output.result, &list);

  REQUIRE(list.buckets().empty());
}

TEST_CASE("ServerAPI::Me should return current permissions") {
  auto repo = ITokenRepository::Build({.data_path = BuildTmpDirectory()});

  ITokenRepository::TokenPermissions permissions;
  permissions.set_full_access(true);
  permissions.mutable_read()->Add("bucket-1");
  permissions.mutable_write()->Add("bucket-2");

  auto [token_value, creat_err] = repo->CreateToken("token-1", permissions);
  REQUIRE(creat_err == Error::kOk);

  auto [receiver, err] = ServerApi::Me(repo.get(), fmt::format("Bearer {}", token_value.value()));
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();
  REQUIRE(output.error == Error::kOk);

  ITokenRepository::Token token;
  JsonStringToMessage(output.result, &token);

  REQUIRE(token.name() == "token-1");
  REQUIRE(token.permissions().full_access());
  REQUIRE(token.permissions().read().size() == 1);
  REQUIRE(token.permissions().read(0) == "bucket-1");
  REQUIRE(token.permissions().write().size() == 1);
  REQUIRE(token.permissions().write(0) == "bucket-2");

  SECTION("invalid token") {
    REQUIRE(ServerApi::Me(repo.get(), "Bearer XXXX").error == Error::Unauthorized("Invalid token"));
  }

  SECTION("no token") {
    REQUIRE(ServerApi::Me(repo.get(), "").error == Error::Unauthorized("No bearer token in request header"));
  }
}
