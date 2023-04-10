// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <optional>
#include <ostream>
#include <vector>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/core/time.h"
#include "reduct/proto/api/entry.pb.h"
#include "reduct/storage/io/async_io.h"
#include "reduct/storage/query/query.h"

namespace reduct::storage {

/**
 *  Entry of a bucket. Store history of blobs as time series
 */
class IEntry : public io::IAsyncIO {
 public:
  using UPtr = std::unique_ptr<IEntry>;
  using SPtr = std::shared_ptr<IEntry>;
  using WPtr = std::weak_ptr<IEntry>;

  virtual ~IEntry() = default;

  /**
   * Options
   */
  struct Options {
    size_t max_block_size;     // max block quota_size after that we create a new one
    size_t max_block_records;  // max number of records in a block

    std::strong_ordering operator<=>(const Options& rhs) const = default;
  };

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
   * @brief Start querying records
   * @param start start point of time interval. If it is nullopt then first record
   * @param stop stop point of time interval. If it is nullopt then last record
   * @param options options
   * @return return query Id
   */
  virtual core::Result<uint64_t> Query(const std::optional<core::Time>& start, const std::optional<core::Time>& stop,
                                       const query::IQuery::Options& options) = 0;

  /**
   * @brief Get next record
   * @param query_id
   * @return information about record to read it
   */
  virtual core::Result<query::IQuery::NextRecord> Next(uint64_t query_id) const = 0;
  /**
   * @brief Creates a new entry.
   * Directory path/name must be empty
   * @param options
   * @return pointer to entre or nullptr if failed to create
   */
  static IEntry::UPtr Build(std::string_view name, const std::filesystem::path& path, Options options);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_ENTRY_H
