// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_SLEEP_H
#define REDUCT_STORAGE_SLEEP_H

#include <chrono>
#include <coroutine>
#include <future>

#include "reduct/async/executors.h"
#include "reduct/async/task.h"

namespace reduct::async {

struct Sleep {
  Sleep(std::chrono::milliseconds delay) : delay_{delay}, start_{std::chrono::steady_clock::now()} {}

  bool await_ready() const noexcept { return decltype(start_)::clock::now() - start_ > delay_; }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    if (decltype(start_)::clock::now() - start_ > delay_) {
      h.resume();
    } else {
      ILoop::loop().Defer([this, h] { await_suspend(h); });
    }
  }

  void await_resume() const noexcept {}

 private:
  std::chrono::milliseconds delay_;
  std::chrono::time_point<std::chrono::steady_clock> start_;
};

template <typename T, typename Executor = LoopExecutor<T>>
struct Run {
  using Us = std::chrono::microseconds;

  Run(std::function<T()>&& task, Executor& executor) { task_ = executor.Commit(std::move(task)); }
  Run(std::function<T()>&& task) { task_ = LoopExecutor<T>().Commit(std::move(task)); }

  bool await_ready() const noexcept { return CheckTask(); }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    if (CheckTask()) {
      h.resume();
    } else {
      ILoop::loop().Defer([this, h] { await_suspend(h); });
    }
  }

  T await_resume() noexcept { return task_.get(); }

 private:
  Run() = default;
  inline bool CheckTask() const { return task_.template wait_for(Us(10)) == std::future_status::ready; }
  std::future<T> task_;
};
}  // namespace reduct::async
#endif  // REDUCT_STORAGE_SLEEP_H
