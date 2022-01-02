// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <ostream>

#include "reduct/core/error.h"

namespace reduct::storage {

/**
 *  Entry of a bucket. Store history of blobs as time series
 */
class IEntry {
 public:
  /**
   * Options
   */
  struct Options {
    std::string name;             // name of entry
    std::filesystem::path path;   // path to entry (path to bucket)
    size_t max_block_size;        // max block size after that we create a new one

    bool operator==(const Options& rhs) const {
      return std::tie(name, path, max_block_size) == std::tie(rhs.name, rhs.path, rhs.max_block_size);
    }

    bool operator!=(const Options& rhs) const { return !(rhs == *this); }
  };

  using Time = std::chrono::system_clock::time_point;

  /**
   * Result of reading
   */
  struct ReadResult {
    std::string data;   // blob of data
    core::Error error;  // error (Error::kOk if no errors)
    Time time;          // timestamp of blob

    bool operator==(const ReadResult& rhs) const {
      return std::tie(data, error, time) == std::tie(rhs.data, rhs.error, rhs.time);
    }

    bool operator!=(const ReadResult& rhs) const { return !(rhs == *this); }

    friend std::ostream& operator<<(std::ostream& os, const ReadResult& result);
  };

  /**
   * Statistic information about the entry
   */
  struct Info {
    size_t block_count;   // stored blocks
    size_t record_count;  // stored records
    size_t bytes;         // stored data size with overhead

    bool operator==(const Info& rhs) const {
      return std::tie(block_count, record_count, bytes) == std::tie(rhs.block_count, rhs.record_count, rhs.bytes);
    }

    bool operator!=(const Info& rhs) const { return !(rhs == *this); }

    friend std::ostream& operator<<(std::ostream& os, const Info& info);
  };

  /**
   * @brief Write a data with timestamp to corresponding block
   * The method provides the best performance if a new timestamp is always new the stored ones.
   * Then the engine doesn't need to find a proper block and just records data into the current one.
   * @param blob data to store
   * @param time timestamp of the data
   * @return error 500 if failed to write data
   */
  [[nodiscard]] virtual core::Error Write(std::string&& blob, const Time& time) = 0;

  /**
   * @brief Finds the record for the timestamp and read the blob
   * Current implementation provide only exact matching.
   * @param time timestamp of record to read
   * @return blob and timestamp of data, or error (404 - if no record found, 500 some internal errors)
   */
  [[nodiscard]] virtual ReadResult Read(const Time& time) const = 0;

  /**
   * @brief Provides statistical information about the entry
   * @return
   */
  [[nodiscard]] virtual Info GetInfo() const = 0;

  /**
   * @brief Provides current options of the entry
   * @return
   */
  [[nodiscard]] virtual const Options& GetOptions() const = 0;


  /**
   * @brief Creates a new entry.
   * Directory path/name must be empty
   * @param options
   * @return pointer to entre or nullptr if failed to create
   */
  static std::unique_ptr<IEntry> Build(Options options);

  /**
   * @brief Restores entry from dir
   * Reads files of settings and descriptor
   * @param full_path path to folder of the entry
   * @return pointer to entre or nullptr if failed to create
   */
  static std::unique_ptr<IEntry> Restore(std::filesystem::path full_path);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_ENTRY_H
