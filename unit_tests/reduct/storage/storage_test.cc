// Copyright 2021 Alexey Timin

#include "reduct/storage/storage.h"

#include <catch2/catch.hpp>

#include "reduct/config.h"

using reduct::api::IApiHandler;
using reduct::storage::IStorage;

TEST_CASE("storage::Storage should provide info about itself", "[storage]") {
  auto storage = IStorage::Build({});

  IApiHandler::InfoResponse info;
  auto err = storage->BindWithApi()->OnInfoRequest(&info);
  REQUIRE_FALSE(err);

  REQUIRE(info.version == reduct::kVersion);
}