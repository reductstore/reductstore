// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/async/task.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/run.h"

using reduct::async::Task;
using reduct::async::VoidTask;

Task<int> Suspended() {
  co_await std::suspend_always();
  co_return 100;
}

Task<int> NotSuspended() {
  co_await std::suspend_never();
  co_return 100;
}

VoidTask VoidSuspended() {
  co_await std::suspend_always();
  co_return;
}

VoidTask VoidNotSuspended() {
  co_await std::suspend_never();
  co_return;
}

TEST_CASE("async::Task should resume and return value", "[task]") {
  auto task = Suspended();
  REQUIRE(task.Get() == 100);
}

TEST_CASE("async::Task should return value", "[task]") {
  auto task = NotSuspended();
  REQUIRE(task.Get() == 100);
}

TEST_CASE("async::VoidTask should run async suspended coro", "[task]") {
  auto task = VoidSuspended();
}

TEST_CASE("async::VoidTask should run async not suspended coro", "[task]") {
  auto task = VoidNotSuspended();
}
