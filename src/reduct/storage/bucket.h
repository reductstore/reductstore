// Copyright 2021-2022 Alexey Timin
#ifndef REDUCT_STORAGE_BUCKET_H
#define REDUCT_STORAGE_BUCKET_H

#include <filesystem>
#include <ostream>

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

    bool operator==(const QuotaOptions& rhs) const { return std::tie(type, size) == std::tie(rhs.type, rhs.size); }
    bool operator!=(const QuotaOptions& rhs) const { return !(rhs == *this); }
    friend std::ostream& operator<<(std::ostream& os, const QuotaOptions& options);
  };

  /**
   * Options used for creating a bucket
   */
  struct Options {
    std::string name;
    std::filesystem::path path;

    size_t max_block_size;
    QuotaOptions quota;

    bool operator==(const Options& rhs) const {
      return std::tie(name, path, max_block_size, quota) == std::tie(rhs.name, rhs.path, max_block_size, rhs.quota);
    }

    bool operator!=(const Options& rhs) const { return !(rhs == *this); }
    friend std::ostream& operator<<(std::ostream& os, const Options& options);
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
    size_t entry_count;   // number of entries in the bucket
    size_t record_count;  // number of records in all the entries of the bucket
    size_t size;          // size of stored in the bucket data in bytes

    bool operator==(const Info& rhs) const {
      return std::tie(entry_count, record_count, size) == std::tie(rhs.entry_count, rhs.record_count, rhs.size);
    }
    bool operator!=(const Info& rhs) const { return !(rhs == *this); }
    friend std::ostream& operator<<(std::ostream& os, const Info& info);
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
