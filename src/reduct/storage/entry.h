// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <ostream>
#include <vector>

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
    std::string name;            // name of entry
    std::filesystem::path path;  // path to entry (path to bucket)
    size_t max_block_size;       // max block quota_size after that we create a new one

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
    std::string blob;   // blob of data
    core::Error error;  // error (Error::kOk if no errors)
    Time time;          // timestamp of blob

    bool operator==(const ReadResult& rhs) const {
      return std::tie(blob, error, time) == std::tie(rhs.blob, rhs.error, rhs.time);
    }

    bool operator!=(const ReadResult& rhs) const { return !(rhs == *this); }

    friend std::ostream& operator<<(std::ostream& os, const ReadResult& result);
  };

  /**
   * Info about a record in a block
   */
  struct RecordInfo {
    Time time;    // time when it was created
    size_t size;  // size in bytes

    bool operator==(const RecordInfo& rhs) const { return std::tie(time, size) == std::tie(rhs.time, rhs.size); }
    bool operator!=(const RecordInfo& rhs) const { return !(rhs == *this); }

    friend std::ostream& operator<<(std::ostream& os, const RecordInfo& info);
  };

  /**
   * Result of the list request
   */
  struct ListResult {
    std::vector<RecordInfo> records;
    core::Error error;
  };

  /**
   * Statistic information about the entry
   */
  struct Info {
    size_t block_count;       // stored blocks
    size_t record_count;      // stored records
    size_t bytes;             // stored data quota_size with overhead
    Time oldest_record_time;  // time of the oldest record
    Time latest_record_time;  // time of the latest record

    bool operator==(const Info& rhs) const {
      return std::tie(block_count, record_count, bytes, oldest_record_time, latest_record_time) ==
             std::tie(rhs.block_count, rhs.record_count, rhs.bytes, oldest_record_time, latest_record_time);
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
  [[nodiscard]] virtual core::Error Write(std::string_view blob, const Time& time) = 0;

  /**
   * @brief Finds the record for the timestamp and read the blob
   * Current implementation provide only exact matching.
   * @param time timestamp of record to read
   * @return blob and timestamp of data, or error (404 - if no record found, 500 some internal errors)
   */
  [[nodiscard]] virtual ReadResult Read(const Time& time) const = 0;

  /**
   * @brief List records for the time interval
   * @param start
   * @param stop
   * @return return time stamps and size of records,  empty if no data
   */
  [[nodiscard]] virtual ListResult List(const Time& start, const Time& stop) const = 0;

  /**
   * @brief Remove the oldest block from disk
   * @return
   */
  virtual core::Error RemoveOldestBlock() = 0;
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
