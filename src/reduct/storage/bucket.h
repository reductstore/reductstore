// Copyright 2021-2022 Alexey Timin
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
  /**
   * Reference to an entry in the bucket
   */
  struct EntryRef {
    std::weak_ptr<IEntry> entry;  // weak pointer because the bucket may remove it
    core::Error error;            // error if failed to create or get an entry
  };
  /**
   * @brief Tries to get an entry by name
   * If there is no entry with the name, the bucket creates one
   * @param name
   * @return error if it failed to create a bucket
   */
  [[nodiscard]] virtual EntryRef GetOrCreateEntry(const std::string& name) = 0;

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
  virtual std::vector<std::string> GetEntryList() const = 0;
  /**
   * @brief Builds a new bucket
   * @param options
   * @return
   */
  static std::unique_ptr<IBucket> Build(std::filesystem::path full_path, proto::api::BucketSettings options = {});

  /**
   * @brief Restores a bucket from folder
   * @param full_path
   * @return
   */
  static std::unique_ptr<IBucket> Restore(std::filesystem::path full_path);
};

}  // namespace reduct::storage
#endif  // REDUCT_STORAGE_BUCKET_H
