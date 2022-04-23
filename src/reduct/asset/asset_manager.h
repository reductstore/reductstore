//
// Created by atimin on 22.04.22.
//

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
   * Creates an asset from ZIP-ed string
   * @param zipped zipped folder with files
   * @return
   */
  static std::unique_ptr<IAssetManager> BuildFromZip(std::string zipped);

  /**
   * Create an empty asset which returns only 404 error
   * @return
   */
  static std::unique_ptr<IAssetManager> BuildEmpty();
};

}  // namespace reduct::asset

#endif  // REDUCT_STORAGE_ASSET_MANAGER_H
