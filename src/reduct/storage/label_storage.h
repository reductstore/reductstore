// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCTSTORE_LABEL_STORAGE_H
#define REDUCTSTORE_LABEL_STORAGE_H

#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/core/time.h"

namespace reduct::storage {

/**
 * @brief LabelStorage is a class that stores labels for records
 */
class ILabelStorage {
 public:
  using UPtr = std::unique_ptr<ILabelStorage>;
  using LabelMap = std::map<std::string, std::string>;

  virtual ~ILabelStorage() = default;

  /**
   * @brief Save labels for a certain record
   * @param time timestamp of the record
   * @param labels labels to save
   * @return error or success
   */
  virtual core::Error SaveLabels(core::Time time, LabelMap labels) = 0;

  /**
   * @brief Get labels for a certain record
   * @param time timestamp of the record
   * @return error or labels
   */
  virtual core::Result<LabelMap> GetLabels(core::Time time) const = 0;

  struct Options {};

  /**
   * @brief Create a new instance of LabelStorage
   * @param path path to the storage
   * @param options options for the storage
   * @return error or instance
   */
  static core::Result<UPtr> Build(std::filesystem::path path);
};

}  // namespace reduct::storage

#endif  // REDUCTSTORE_LABEL_STORAGE_H
