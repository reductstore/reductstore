// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <ostream>
#include <vector>

#include "reduct/async/io.h"
#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/api/entry.pb.h"

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

    bool operator<=>(const Options& rhs) const = default;
  };

  using Time = std::chrono::system_clock::time_point;

  /**
   * Info about a record in a block
   */
  struct RecordInfo {
    Time time;    // time when it was created
    size_t size;  // size in bytes

    bool operator<=>(const RecordInfo& rhs) const = default;

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
   * @brief Write a data with timestamp to corresponding block
   * The method provides the best performance if a new timestamp is always new the stored ones.
   * Then the engine doesn't need to find a proper block and just records data into the current one.
   * @param time timestamp of the data
   * @return async writer or error
   */
  [[nodiscard]] virtual core::Result<async::IAsyncWriter::UPtr> BeginWrite(const Time& time, size_t size) = 0;

  /**
   * @brief Finds the record for the timestamp and read the blob
   * Current implementation provide only exact matching.
   * @param time timestamp of record to read
   * @return async reader or error (404 - if no record found, 500 some internal errors)
   */
  [[nodiscard]] virtual core::Result<async::IAsyncReader::UPtr> BeginRead(const Time& time) const = 0;

  /**
   * @brief List records for the time interval [start, stop)
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
  [[nodiscard]] virtual proto::api::EntryInfo GetInfo() const = 0;

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
