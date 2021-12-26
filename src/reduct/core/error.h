// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_ERROR_H
#define REDUCT_STORAGE_ERROR_H

#include <string>
#include <ostream>
#include <variant>

namespace reduct::core {

struct [[nodiscard]]  Error {
  int code = 0;
  std::string message{};

  operator bool() const;

  std::string ToString() const;

  bool operator==(const Error& rhs) const;

  bool operator!=(const Error& rhs) const;
};

}  // namespace reduct::core
#endif  // REDUCT_STORAGE_ERROR_H
