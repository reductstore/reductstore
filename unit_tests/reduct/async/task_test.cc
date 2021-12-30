// Copyright 2021 Alexey Timin

#include "reduct/async/task.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/run.h"

using reduct::async::Task;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

Task<int> Suspended() {
  co_await std::suspend_always();
  co_return 100;
};

Task<int> NotSuspended() {
  co_await std::suspend_never();
  co_return 100;
};

TEST_CASE("async::Task should resume and return value") {
  auto task = Suspended();
  LOG_ERROR("!!!");
  REQUIRE(task.Get() == 100);
}

TEST_CASE("async::Task should return value") {
  auto task = NotSuspended();
  LOG_ERROR("!!!");

  REQUIRE(task.Get() == 100);
}