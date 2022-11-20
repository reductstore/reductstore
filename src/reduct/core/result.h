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
  Result() = default;
  Result(T result) : result(std::move(result)), error(Error::kOk) {}
  Result(Error error) : result(), error(std::move(error)) {}
  Result(T result, Error error) : result(std::move(result)), error(std::move(error)) {}

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

}  // namespace reduct::core

#endif  // REDUCT_CORE_RESULT_H
