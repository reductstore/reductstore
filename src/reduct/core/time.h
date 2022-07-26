// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TIME_H
#define REDUCT_STORAGE_TIME_H

#include <chrono>

namespace reduct::core {
using Time = std::chrono::system_clock::time_point;
}
#endif  // REDUCT_STORAGE_TIME_H
