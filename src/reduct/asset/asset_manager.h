// Copyright 2022 Reduct Storage Team

#ifndef REDUCT_STORAGE_ASSET_MANAGER_H
#define REDUCT_STORAGE_ASSET_MANAGER_H
#include <libzippp/libzippp.h>

#include <filesystem>
#include <string>

#include "reduct/core/result.h"

namespace reduct::asset {

/**
 * Helper class to get access to static assets
 */
class IAssetManager {
 public:
  virtual ~IAssetManager() = default;

  /**
   * Read an asset by its relative path
   * @param relative_path
   * @return
   */
  virtual core::Result<std::string> Read(std::string_view relative_path) const = 0;

  /**
   * Creates an asset from ZIP-ed string in hex format
   * @param zipped zipped folder with files
   * @return
   */
  static std::unique_ptr<IAssetManager> BuildFromZip(std::string_view zipped);

  /**
   * Create an empty asset which returns only 404 error
   * @return
   */
  static std::unique_ptr<IAssetManager> BuildEmpty();
};

}  // namespace reduct::asset

#endif  // REDUCT_STORAGE_ASSET_MANAGER_H
