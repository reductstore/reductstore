// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <ostream>
#include <vector>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/api/entry.pb.h"
#include "reduct/storage/io/async_io.h"
#include "reduct/storage/query/quiery.h"

namespace reduct::storage {

/**
 *  Entry of a bucket. Store history of blobs as time series
 */
class IEntry : public io::IAsyncIO, public query::IQuery {
 public:
  /**
   * Options
   */
  struct Options {
    size_t max_block_size;     // max block quota_size after that we create a new one
    size_t max_block_records;  // max number of records in a block

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
   * @brief List records for the time interval [start, stop)
   * @param start
   * @param stop
   * @return return time stamps and size of records,  empty if no data
   */
  [[nodiscard]] virtual core::Result<std::vector<RecordInfo>> List(const Time& start, const Time& stop) const = 0;

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
   * @brief Set options
   * @return
   */
  virtual void SetOptions(const Options&) = 0;

  /**
   * @brief Creates a new entry.
   * Directory path/name must be empty
   * @param options
   * @return pointer to entre or nullptr if failed to create
   */
  static std::unique_ptr<IEntry> Build(std::string_view name, const std::filesystem::path& path, Options options);

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
