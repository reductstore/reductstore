// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/core/error.h"

#include <fmt/core.h>

namespace reduct::core {

const Error Error::kOk = Error{};

Error::operator bool() const { return code >= 300 || code < 100; }

std::string Error::ToString() const { return fmt::format("[{}] {}", code, message); }

std::ostream& operator<<(std::ostream& os, const Error& error) {
  os << error.ToString();
  return os;
}

}  // namespace reduct::core
