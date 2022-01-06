// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_ERROR_H
#define REDUCT_STORAGE_ERROR_H

#include <ostream>
#include <string>
#include <variant>

namespace reduct::core {

struct [[nodiscard]] Error { // NOLINT
  int code = 0;
  std::string message{};

  operator bool() const;

  std::string ToString() const;

  bool operator==(const Error& rhs) const;
  bool operator!=(const Error& rhs) const;
  friend std::ostream& operator<<(std::ostream& os, const Error& error);

  static const Error kOk;
};

}  // namespace reduct::core
#endif  // REDUCT_STORAGE_ERROR_H
