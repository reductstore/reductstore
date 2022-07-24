// Copyright 2021 Alexey Timin

#include "reduct/core/error.h"

#include <fmt/core.h>

namespace reduct::core {

const Error Error::kOk = Error{};

Error::operator bool() const { return code >= 300 || code < 200; }

std::string Error::ToString() const { return fmt::format("[{}] {}", code, message); }

std::ostream& operator<<(std::ostream& os, const Error& error) {
  os << error.ToString();
  return os;
}

}  // namespace reduct::core
