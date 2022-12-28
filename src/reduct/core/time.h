// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_TIME_H
#define REDUCT_STORAGE_TIME_H

#include <chrono>

namespace reduct::core {
using Time = std::chrono::system_clock::time_point;

inline auto ToMicroseconds(Time tp) {
  return std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()).count();
}

}
#endif  // REDUCT_STORAGE_TIME_H
