// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/api/server_api.h"

#include <catch2/catch.hpp>
#include <nlohmann/json.hpp>

#include "reduct/helpers.h"

using google::protobuf::util::JsonStringToMessage;
using reduct::api::ServerApi;
using reduct::core::Error;
using reduct::storage::IStorage;

namespace rs = reduct::rust_part;

TEST_CASE("ServerApi::Alive should return empty body", "[api]") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [receiver, err] = ServerApi::Alive(storage.get());
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();
  REQUIRE(output.error == Error::kOk);
  REQUIRE(output.result.empty());
}

TEST_CASE("ServerApi::Info should return JSON", "[api]") {
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

TEST_CASE("ServerApi::List should return JSON", "[api]") {
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

TEST_CASE("ServerAPI::Me should return current permissions", "[api]") {
  auto repo = rs::new_token_repo(BuildTmpDirectory().string(), "init-token");

  auto permissions = rs::new_token_permissions(true, {"bucket-1"}, {"bucket-2"});
  auto answer = rs::new_token_create_response();
  REQUIRE(rs::token_repo_create_token(*repo, "token-1", *permissions, *answer)->status() == 200);

  std::string value = nlohmann::json::parse(rs::token_create_response_to_json(*answer))["value"];
  auto [receiver, err] = ServerApi::Me(*repo, fmt::format("Bearer {}", value));
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();
  REQUIRE(output.error == Error::kOk);

  auto token = rs::new_token();

  auto json = nlohmann::json::parse(output.result);

  REQUIRE(json["name"] == "token-1");
  REQUIRE(json["permissions"]["full_access"] == true);
  REQUIRE(json["permissions"]["read"].size() == 1);
  REQUIRE(json["permissions"]["read"][0] == "bucket-1");
  REQUIRE(json["permissions"]["write"].size() == 1);
  REQUIRE(json["permissions"]["write"][0] == "bucket-2");

  SECTION("invalid token") {
    REQUIRE(ServerApi::Me(*repo, "Bearer XXXX").error == Error::Unauthorized("Invalid token"));
  }

  SECTION("no token") {
    REQUIRE(ServerApi::Me(*repo, "").error == Error::Unauthorized("No bearer token in request header"));
  }
}

TEST_CASE("ServerAPI::Me should return placeholder always and authentication is disabled", "[api]") {
  auto repo = rs::new_token_repo(BuildTmpDirectory().string(), "");

  auto [receiver, err] = ServerApi::Me(*repo, "invalid-token");
  REQUIRE(err == Error::kOk);

  auto [resp, recv_err] = receiver("", true);
  REQUIRE(recv_err == Error::kOk);

  auto output = resp.SendData();
  REQUIRE(output.error == Error::kOk);

  auto json = nlohmann::json::parse(output.result);

  REQUIRE(json["name"] == "AUTHENTICATION-DISABLED");
  REQUIRE(json["permissions"]["full_access"] == true);
  REQUIRE(json["permissions"]["read"].size() == 0);
  REQUIRE(json["permissions"]["write"].size() == 0);
}
