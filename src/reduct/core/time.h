// Copyright 2022 Alexey Timin

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
