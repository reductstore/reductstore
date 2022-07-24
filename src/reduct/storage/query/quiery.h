// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_QUIERY_H
#define REDUCT_STORAGE_QUIERY_H

#include <chrono>
#include <optional>

#include "reduct/core/result.h"

namespace reduct::storage::query {

class IQuery {
 public:
  using Time = std::chrono::system_clock::time_point;

  /**
   * Query Options
   */
  struct Options {
    std::chrono::seconds ttl{5};  // TTL of query in entries cache (time from last request)
  };

  /**
   * @brief Query records for time interval
   * @param start start point of time interval. If it is nullopt then first record
   * @param stop stop point of time interval. If it is nullopt then last record
   * @param options options
   * @return return query Id
   */
  [[nodiscard]] virtual core::Result<uint64_t> Query(const std::optional<Time>& start, const std::optional<Time>& stop,
                                                     const Options& options) = 0;
  /**
   * Information about record
   */
  struct NextRecord {
    Time time;
    size_t size{};
    bool last{};

    bool operator<=>(const NextRecord&) const = default;
  };

  /**
   * @brief Get next
   * @param query_id
   * @return information about record to read it
   */
  [[nodiscard]] virtual core::Result<NextRecord> Next(uint64_t query_id) const = 0;
};

}  // namespace reduct::storage::query
#endif  // REDUCT_STORAGE_QUIERY_H
