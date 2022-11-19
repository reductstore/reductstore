// Copyright 2021 Alexey Timin

#ifndef REDUCT_CORE_ERROR_H
#define REDUCT_CORE_ERROR_H

#include <ostream>
#include <string>
#include <variant>

namespace reduct::core {

/**
 * Error with code and message
 */
struct [[nodiscard]] Error {  // NOLINT
  int code = 200;
  std::string message{};

  /**
   * true if there is an error
   * @return
   */
  operator bool() const;

  std::string ToString() const;

  std::strong_ordering operator<=>(const Error& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& os, const Error& error);

  /**
   * Use Error::kOk to avoid creating an object
   */
  static const Error kOk;

  // HTTP codes 100-200
  static Error Continue(std::string msg = "Continue") { return Error{100, std::move(msg)}; }

  // HTTP codes 200-300
  // HTTP codes 300-400
  // HTTP codes 400-500
  static Error NotFound(std::string msg = "NotFound") { return Error{404, std::move(msg)}; }
  static Error Conflict(std::string msg = "Conflict") { return Error{409, std::move(msg)}; }
};

}  // namespace reduct::core
#endif  // REDUCT_CORE_ERROR_H
