// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ASYNC_IO_H
#define REDUCT_STORAGE_ASYNC_IO_H

#include <chrono>

#include "reduct/async/io.h"

namespace reduct::storage::io {

class IAsyncIO {
 public:
  using Time = std::chrono::system_clock::time_point;

  /**
   * @brief Write a data with timestamp to corresponding block
   * The method provides the best performance if a new timestamp is always new the stored ones.
   * Then the engine doesn't need to find a proper block and just records data into the current one.
   * @param time timestamp of the data
   * @return async writer or error
   */
  [[nodiscard]] virtual core::Result<async::IAsyncWriter::SPtr> BeginWrite(const Time& time, size_t size) = 0;

  /**
   * @brief Finds the record for the timestamp and read the blob
   * Current implementation provide only exact matching.
   * @param time timestamp of record to read
   * @return async reader or error (404 - if no record found, 500 some internal errors)
   */
  [[nodiscard]] virtual core::Result<async::IAsyncReader::SPtr> BeginRead(const Time& time) const = 0;
};
}  // namespace reduct::storage::io
#endif  // REDUCT_STORAGE_ASYNC_IO_H
