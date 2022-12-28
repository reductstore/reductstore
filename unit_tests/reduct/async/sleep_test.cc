// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/async/sleep.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/task.h"

using reduct::async::ILoop;
using reduct::async::Sleep;
using reduct::async::Task;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals; // NOLINT

Task<int> SleepCoro(std::chrono::milliseconds sleep) {
  co_await Sleep(sleep);
  co_return 100;
}

TEST_CASE("async::Sleep should sleep in loop") {
  auto start = Clock::now();
  auto task = SleepCoro(50ms);

  REQUIRE(task.Get() == 100);
  REQUIRE(Clock::now() - start > 50ms);
}
