// Copyright 2021 Alexey Timin

#ifndef REDUCT_ASYNC_SLEEP_H
#define REDUCT_ASYNC_SLEEP_H

#include <chrono>
#include <coroutine>
#include <future>

#include "reduct/async/executors.h"
#include "reduct/async/task.h"

namespace reduct::async {

template <typename R, typename P>
struct Sleep {
  Sleep(std::chrono::duration<R, P> delay) : delay_{delay}, start_{std::chrono::steady_clock::now()} {}

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
  std::chrono::duration<R, P> delay_;
  std::chrono::time_point<std::chrono::steady_clock> start_;
};

}  // namespace reduct::async
#endif  // REDUCT_ASYNC_SLEEP_H
