// Copyright 2021 Alexey Timin

#include "reduct/async/sleep.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/task.h"

using reduct::async::ILoop;
using reduct::async::Sleep;
using reduct::async::Task;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

Task<int> sleep_coro(std::chrono::milliseconds sleep) {
  co_await Sleep(sleep);
  co_return 100;
};

TEST_CASE("async::Sleep should sleep in loop") {
  auto start = Clock::now();
  auto task = sleep_coro(50ms);

  REQUIRE(task.WaitFor(60ms) == 100);
  REQUIRE(Clock::now() - start > 50ms);
}
