// Copyright 2021 Alexey Timin

#include "reduct/async/run.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/task.h"

using reduct::async::Run;
using reduct::async::Task;

template <typename T>
struct SimpleThreadExecutor {
  std::future<T> Commit(std::function<T()>&& task) {
    std::packaged_task<int()> wrapper([t = std::move(task)] { return t(); });
    std::future<int> future = wrapper.get_future();
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
