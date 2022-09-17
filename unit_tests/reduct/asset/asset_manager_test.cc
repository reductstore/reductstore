// Copyright 2022 Reduct Storage Team

#include "reduct/asset/asset_manager.h"

#include <catch2/catch.hpp>
#include <libzippp/libzippp.h>

#include <fstream>

#ifdef WITH_CONSOLE
#include "reduct/console.h"
#endif

using reduct::asset::IAssetManager;
using reduct::core::Error;

TEST_CASE("asset::IAssetManager should have empty implementation") {
  auto empty = IAssetManager::BuildEmpty();
  REQUIRE(empty->Read("/").error == Error{.code = 404, .message = "No static files supported"});
}

#if WITH_CONSOLE
TEST_CASE("asset::IAssetManager should extract files form zip") {
  auto zip_asset = IAssetManager::BuildFromZip(std::string(reduct::kZippedConsole));

  REQUIRE(zip_asset);
  REQUIRE(zip_asset->Read("index.html").result.size() > 0);
  REQUIRE(zip_asset->Read("noexist").error == Error{.code = 404, .message = "File 'noexist' not found"});
}
#endif
