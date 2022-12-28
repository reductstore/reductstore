// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#ifndef REDUCT_STORAGE_BUCKET_H
#define REDUCT_STORAGE_BUCKET_H

#include <filesystem>
#include <ostream>

#include "reduct/proto/api/bucket.pb.h"
#include "reduct/storage/entry.h"

namespace reduct::storage {

/**
 * Represents a buck in the storage ingine
 */
class IBucket {
 public:
  using WPtr = std::weak_ptr<IBucket>;
  using SPtr = std::shared_ptr<IBucket>;
  using UPtr = std::unique_ptr<IBucket>;

  /**
   * @brief Tries to get an entry by name
   * If there is no entry with the name, the bucket creates one
   * @param name
   * @return error if it failed to create a bucket
   */
  [[nodiscard]] virtual core::Result<IEntry::WPtr> GetOrCreateEntry(const std::string& name) = 0;

  /**
   * @brief Remove data of all the buckets
   * @return
   */
  [[nodiscard]] virtual core::Error Clean() = 0;

  /**
   * @brief Bucket checks if it has data more than quota and remove some data
   * Depends on quota type:
   * kNone - does nothing
   * kFifo - removes the oldest block in the bucket
   * @return error 500 if something goes wrong
   */
  [[nodiscard]] virtual core::Error KeepQuota() = 0;

  /**
   * @brief SetS bucket settings and save in descriptor
   * @note It doesnt change name and path
   * @param options
   * @return
   */
  virtual core::Error SetSettings(proto::api::BucketSettings options) = 0;
  /**
   * @brief Returns statistics about the bucket
   * @return
   */
  [[nodiscard]] virtual proto::api::BucketInfo GetInfo() const = 0;

  /**
   * @brief Returns options of the bucket
   * @return
   */
  [[nodiscard]] virtual const proto::api::BucketSettings& GetSettings() const = 0;

  /**
   * Return list of entry names
   * @return
   */
  virtual std::vector<proto::api::EntryInfo> GetEntryList() const = 0;

  virtual bool HasEntry(const std::string& name) const = 0;
  /**
   * @brief Builds a new bucket
   * @param options
   * @return
   */

  static IBucket::UPtr Build(std::filesystem::path full_path, proto::api::BucketSettings options = {});

  /**
   * @brief Restores a bucket from folder
   * @param full_path
   * @return
   */
  static IBucket::UPtr Restore(std::filesystem::path full_path);

  /**
   * Gets default settings for a new bucket
   * @return
   */
  static const proto::api::BucketSettings& GetDefaults();
};

}  // namespace reduct::storage
#endif  // REDUCT_STORAGE_BUCKET_H
