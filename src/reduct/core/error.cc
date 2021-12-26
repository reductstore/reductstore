// Copyright 2021 Alexey Timin

#include "reduct/core/error.h"

#include <fmt/core.h>

namespace reduct::core {

Error::operator bool() const { return code != 0; }

std::string Error::ToString() const { return fmt::format("[{}] {}", code, message); }

bool Error::operator==(const Error& rhs) const { return code == rhs.code && message == rhs.message; }

bool Error::operator!=(const Error& rhs) const { return !(rhs == *this); }

}  // namespace reduct::core