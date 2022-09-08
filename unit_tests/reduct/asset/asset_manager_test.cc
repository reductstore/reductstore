// Copyright 2022 Reduct Storage Team

#include "reduct/asset/asset_manager.h"

#include <catch2/catch.hpp>
#include <libzippp/libzippp.h>

#include <fstream>

using reduct::asset::IAssetManager;
using reduct::core::Error;

TEST_CASE("asset::IAssetManager should have empty implementation") {
  auto empty = IAssetManager::BuildEmpty();
  REQUIRE(empty->Read("/").error == Error{.code = 404, .message = "No static files supported"});
}

TEST_CASE("asset::IAssetManager should extract files form zip") {
  using libzippp::ZipArchive;

  const auto tmp = std::filesystem::temp_directory_path();
  ZipArchive zf((tmp / "archive.zip").string());
  zf.open(ZipArchive::Write);
  zf.addData("helloworld.txt", "Hello,World!", 12);
  zf.close();

  std::ifstream arch(tmp / "archive.zip");
  std::stringstream ss;
  ss << arch.rdbuf();

  auto zip_asset = IAssetManager::BuildFromZip(ss.str());

  REQUIRE(zip_asset);
  REQUIRE(zip_asset->Read("helloworld.txt").result == "Hello,World!");
  REQUIRE(zip_asset->Read("noexist").error == Error{.code = 404, .message = "File 'noexist' not found"});
}
