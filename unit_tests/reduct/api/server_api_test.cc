// Copyright 2022 Alexey Timin

#include "reduct/api/server_api.h"

#include <catch2/catch.hpp>

#include "reduct/helpers.h"

using google::protobuf::util::JsonStringToMessage;
using reduct::api::ServerApi;
using reduct::core::Error;
using reduct::storage::IStorage;

TEST_CASE("ServerApi::Alive should return empty body") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [resp, err] = ServerApi::Alive(storage.get());

  REQUIRE(err == Error::kOk);
  auto output = resp.output_call();

  REQUIRE(output.error == Error::kOk);
  REQUIRE(output.result.empty());
}

TEST_CASE("ServerApi::Info should return JSON") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [resp, err] = ServerApi::Info(storage.get());

  REQUIRE(err == Error::kOk);
  auto output = resp.output_call();

  REQUIRE(output.error == Error::kOk);

  reduct::proto::api::ServerInfo info;
  JsonStringToMessage(output.result, &info);
  REQUIRE(info.version() == storage->GetInfo().result.version());
}

TEST_CASE("ServerApi::List should return JSON") {
  auto storage = IStorage::Build({.data_path = BuildTmpDirectory()});

  auto [resp, err] = ServerApi::List(storage.get());

  REQUIRE(err == Error::kOk);
  auto output = resp.output_call();

  REQUIRE(output.error == Error::kOk);

  reduct::proto::api::BucketInfoList list;
  JsonStringToMessage(output.result, &list);

  REQUIRE(list.buckets().empty());
}
