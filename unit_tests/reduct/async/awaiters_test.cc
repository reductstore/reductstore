// Copyright 2021 Alexey Timin

#include "reduct/async/awaiters.h"

#include <catch2/catch.hpp>

#include <chrono>

#include "reduct/async/task.h"

using reduct::async::ILoop;
using reduct::async::Run;
using reduct::async::Sleep;
using reduct::async::Task;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;


Task<int> sleep_coro(std::chrono::milliseconds sleep) {
  co_await Sleep(sleep);
  co_return 100;
};

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

Task<int> run_coro(std::function<int()>&& task) {
  SimpleThreadExecutor<int> executor;
  co_return co_await Run(std::move(task), executor);
};

Task<int> run_in_loop(std::function<int()>&& task) { co_return co_await Run(std::move(task)); };

TEST_CASE("async::Sleep should sleep in loop") {
  auto start = Clock::now();
  auto task = sleep_coro(50ms);

  REQUIRE(task.WaitFor(60ms) == 100);
  REQUIRE(Clock::now() - start > 50ms);
}

TEST_CASE("async::Run should run task in executor") {
  auto start = Clock::now();
  auto task = run_coro([] { return 100; });

  REQUIRE(task.WaitFor(10ms) == 100);
}

TEST_CASE("async::Run should run task in loop by default") {
  auto task = run_in_loop([] {
    return 100;
  });

  REQUIRE(task.WaitFor(100ms) == 100);
}