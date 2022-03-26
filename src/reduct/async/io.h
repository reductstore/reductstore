// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_IO_H
#define REDUCT_STORAGE_IO_H

#include <memory>

#include "reduct/core/error.h"

namespace reduct::async {
class IAsyncWriter {
 public:
  using UPtr = std::unique_ptr<IAsyncWriter>;

  virtual core::Error Write(std::string_view chunk, bool last = true) noexcept = 0;
};
}  // namespace reduct::async
#endif  // REDUCT_STORAGE_IO_H
