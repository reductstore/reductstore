// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/async/run.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/task.h"

using reduct::async::Run;
using reduct::async::RunUntil;
using reduct::async::Task;

template <typename T>
struct SimpleThreadExecutor {
  std::future<T> Commit(std::function<T()> task) {
    std::packaged_task<T()> wrapper([t = std::move(task)] { return t(); });
    std::future<T> future = wrapper.get_future();
    std::thread t(std::move(wrapper));
    t.detach();
    return future;
  }
};

Task<int> RunCoro(std::function<int()>&& task) {
  SimpleThreadExecutor<int> executor;
  co_return co_await Run(std::move(task), &executor);
}

Task<int> RunInLoop(std::function<int()>&& task) { co_return co_await Run(std::move(task)); }

TEST_CASE("async::Run should run task in executor") {
  auto task = RunCoro([] { return 100; });
  REQUIRE(task.Get() == 100);
}

TEST_CASE("async::Run should run task in loop by default") {
  auto task = RunInLoop([] { return 100; });
  REQUIRE(task.Get() == 100);
}

Task<int> RunUntilCoro(std::function<std::optional<int>()>&& task) {
  SimpleThreadExecutor<std::optional<int>> executor;
  co_return co_await RunUntil(std::move(task), &executor);
}

Task<int> RunUntilInLoop(std::function<std::optional<int>()>&& task) { co_return co_await RunUntil(std::move(task)); }

TEST_CASE("async::RunUntil should run task in executor") {
  auto task = RunUntilCoro([]() -> std::optional<int> {
    static int count = 0;
    if (++count < 20) {
      return {};
    }
    return count;
  });

  REQUIRE(task.Get() == 20);
}

TEST_CASE("async::RunUntil should run task in loop by default") {
  auto func = []() -> std::optional<int> {
    static int count = 0;
    if (++count < 20) {
      return {};
    }
    return count;
  };

  auto task = RunUntilCoro(func);
  REQUIRE(task.Get() == 20);
}
