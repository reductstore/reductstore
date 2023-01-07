// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

#define RESULT_OR_RETURN_ERROR(val, expr) \
  {                                       \
    auto result = (expr);                 \
    if (result.error) {                   \
      return result.error;                \
    }                                     \
    val = std::move(result.result);       \
  }
}  // namespace reduct::core

#endif  // REDUCT_CORE_RESULT_H
