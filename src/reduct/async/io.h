// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_IO_H
#define REDUCT_STORAGE_IO_H

#include <memory>
#include <map>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/core/time.h"

namespace reduct::async {

/**
 * @brief Represents an async writer which takes chunks from the loop and write them into a data block
 */
class IAsyncWriter {
 public:
  using UPtr = std::unique_ptr<IAsyncWriter>;
  using SPtr = std::shared_ptr<IAsyncWriter>;

  virtual ~IAsyncWriter() = default;
  virtual core::Error Write(std::string_view chunk, bool last = true) noexcept = 0;
  [[nodiscard]] virtual bool is_done() const noexcept = 0;
};

/**
 * @brief Represents an async reader
 */
class IAsyncReader {
 public:
  virtual ~IAsyncReader() = default;

  using UPtr = std::unique_ptr<IAsyncReader>;
  using SPtr = std::shared_ptr<IAsyncReader>;

  struct DataChunk {
    std::string data;
    bool last;

    std::strong_ordering operator<=>(const DataChunk&) const = default;
  };

  /**
   * @brief Read a chunk of data from the reader
   * @return
   */
  virtual core::Result<DataChunk> Read() noexcept = 0;

  [[nodiscard]] virtual bool is_done() const noexcept = 0;
  [[nodiscard]] virtual core::Time timestamp() const noexcept = 0;
  [[nodiscard]] virtual size_t size() const noexcept = 0;
  [[nodiscard]] virtual const std::map<std::string, std::string>& labels() const noexcept = 0;
};

}  // namespace reduct::async
#endif  // REDUCT_STORAGE_IO_H
