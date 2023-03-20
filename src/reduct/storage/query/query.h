// Copyright 2022-2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_QUIERY_H
#define REDUCT_STORAGE_QUIERY_H

#include <chrono>
#include <optional>

#include "reduct/async/io.h"
#include "reduct/core/result.h"
#include "reduct/core/time.h"
#include "reduct/storage/block_manager.h"

namespace reduct::storage::query {

class IQuery {
 public:
  ~IQuery() = default;

  using UPtr = std::unique_ptr<IQuery>;

  /**
   * Query Options
   */
  struct Options {
    std::chrono::seconds ttl{5};                 // TTL of query in entries cache (time from last request)
    std::map<std::string, std::string> include;  // include labels with certain values
    std::map<std::string, std::string> exclude;  // exclude labels with certain values
  };

  /**
   * @brief Create a query
   * @param start start point of time interval. If it is nullopt then first record
   * @param stop stop point of time interval. If it is nullopt then last record
   * @param block_manager block manager
   * @param options options
   * @return return query Id
   */
  [[nodiscard]] static core::Result<UPtr> Build(const std::optional<core::Time>& start,
                                                const std::optional<core::Time>& stop, const Options& options);
  /**
   * Information about record
   */
  struct NextRecord {
    async::IAsyncReader::SPtr reader;
    bool last{};

    std::strong_ordering operator<=>(const NextRecord&) const = default;
  };

  /**
   * @brief Get next record
   * @param blocks set of blocks to read
   * @param block_manager block manager
   * @return information about record to read it
   */
  [[nodiscard]] virtual core::Result<NextRecord> Next(const std::set<google::protobuf::Timestamp>& blocks,
                                                      IBlockManager* block_manager) = 0;

  virtual bool is_outdated() const = 0;
};

}  // namespace reduct::storage::query
#endif  // REDUCT_STORAGE_QUIERY_H
