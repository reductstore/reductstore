// Copyright 2022 ReductStore Team

#include "reduct/asset/asset_manager.h"

#include <fmt/core.h>
#include <libzippp/libzippp.h>

#include <fstream>
#include <sstream>

#include "reduct/core/logger.h"

namespace reduct::asset {

using core::Result;
namespace fs = std::filesystem;

class AssetManager : public IAssetManager {
 public:
  explicit AssetManager(fs::path asset_path) : asset_path_(std::move(asset_path)) {}

  ~AssetManager() override = default;

  Result<std::string> Read(std::string_view relative_path) const override {
    auto full_path = asset_path_ / relative_path;
    if (!fs::exists(full_path)) {
      return {{}, {.code = 404, .message = fmt::format("File '{}' not found", relative_path)}};
    }

    std::ifstream file(full_path);
    if (!file) {
      return {{}, {.code = 500, .message = fmt::format("Failed to open '{}'", relative_path)}};
    }

    std::stringstream ss;
    ss << file.rdbuf();
    return {ss.str(), core::Error::kOk};
  }

 private:
  fs::path asset_path_;
};

std::unique_ptr<IAssetManager> IAssetManager::BuildFromZip(std::string_view zipped) {
  using libzippp::ZipArchive;

  auto hex_str_to_str = [](std::string hex) {
    auto len = hex.length();
    std::transform(hex.begin(), hex.end(), hex.begin(), toupper);
    std::string new_string;
    for (int i = 0; i < len; i += 2) {
      auto byte = hex.substr(i, 2);
      char chr = ((byte[0] - (byte[0] < 'A' ? 0x30 : 0x37)) << 4) + (byte[1] - (byte[1] < 'A' ? 0x30 : 0x37));
      new_string.push_back(chr);
    }

    return new_string;
  };

  auto binary_string = hex_str_to_str(std::string(zipped));
  auto zf = std::unique_ptr<ZipArchive>(ZipArchive::fromBuffer(binary_string.data(), binary_string.size()));

  if (!zf) {
    LOG_ERROR("Failed to extract asset");
    return nullptr;
  }

  fs::path asset_path = fs::temp_directory_path() /
                        fmt::format("asset_{}", std::chrono::high_resolution_clock::now().time_since_epoch().count());
  fs::create_directory(asset_path);
  std::string root;
  for (auto&& entry : zf->getEntries()) {
    if (entry.isDirectory()) {
      if (root.empty()) {
        root = entry.getName();
      } else {
        fs::create_directory(asset_path / entry.getName().substr(root.size()));
      }
    }

    if (entry.isFile()) {
      auto path = asset_path / entry.getName().substr(root.size());
      std::ofstream out(path);
      if (!out) {
        LOG_ERROR("Failed open file {}: {}", path.string(), std::strerror(errno));
      }
      entry.readContent(out);
    }
  }

  zf->close();
  return std::make_unique<AssetManager>(asset_path);
}

class EmptyAssetManager : public IAssetManager {
 public:
  ~EmptyAssetManager() override = default;

  Result<std::string> Read(std::string_view relative_path) const override {
    return {{}, {.code = 404, .message = "No static files supported"}};
  }
};

std::unique_ptr<IAssetManager> IAssetManager::BuildEmpty() { return std::make_unique<EmptyAssetManager>(); }

}  // namespace reduct::asset
