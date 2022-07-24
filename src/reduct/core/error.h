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

  bool operator<=>(const Error& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& os, const Error& error);

  /**
   * Use Error::kOk to avoid creating an object
   */
  static const Error kOk;
};

}  // namespace reduct::core
#endif  // REDUCT_CORE_ERROR_H
