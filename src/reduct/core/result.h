// Copyright 2022 Alexey Timin
#ifndef REDUCT_CORE_RESULT_H
#define REDUCT_CORE_RESULT_H

#include <memory>

#include "reduct/core/error.h"

namespace reduct::core {

/**
 * Result with error request
 */
template <typename T>
struct [[nodiscard]] Result {  // NOLINT
  T result;
  Error error;  // error code is HTTP status or -1 if it is communication error

  operator const Error&() const noexcept { return error; }
};

/**
 * Result with error request
 */
template <typename T>
struct [[nodiscard]] UPtrResult {  // NOLINT
  std::unique_ptr<T> result;
  Error error;  // error code is HTTP status or -1 if it is communication error

  operator const Error&() const noexcept { return error; }
};

}  // namespace reduct

#endif  // REDUCT_CORE_RESULT_H
