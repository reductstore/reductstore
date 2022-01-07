// Copyright 2021-2022 Alexey Timin
#ifndef REDUCT_STORAGE_BUCKET_H
#define REDUCT_STORAGE_BUCKET_H

#include <filesystem>

#include "reduct/storage/entry.h"

namespace reduct::storage {

/**
 * Represents a buck in the storage ingine
 */
class IBucket {
 public:
  enum QuotaType { kNone = 1, kFifo = 2 };

  struct QuotaOptions {
    QuotaType type;
    size_t size;
  };

  /**
   * Options used for creating a bucket
   */
  struct Options {
    std::string name;
    std::filesystem::path path;

    QuotaOptions quota;
  };

  /**
   * Reference to an entry in the bucket
   */
  struct EntryRef {
    std::weak_ptr<IEntry> entry;  // weak pointer because the bucket may remove it
    core::Error error;            // error if failed to create or get an entry
  };

  /**
   * Statistical information about a bucket
   */
  struct Info {
    size_t entry_count;  // number of entries in the bucket
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
   * @brief Returns statistics about the bucket
   * @return
   */
  [[nodiscard]] virtual Info GetInfo() const = 0;

  /**
   * @brief Returns options of the bucket
   * @return
   */
  [[nodiscard]] virtual const Options& GetOptions() const = 0;

  /**
   * @brief Builds a new bucket
   * @param options
   * @return
   */
  static std::unique_ptr<IBucket> Build(const Options& options);

  /**
   * @brief Restores a bucket from folder
   * @param full_path
   * @return
   */
  static std::unique_ptr<IBucket> Restore(std::filesystem::path full_path);

};

}  // namespace reduct::storage
#endif  // REDUCT_STORAGE_BUCKET_H
