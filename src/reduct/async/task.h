// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_TASK_H
#define REDUCT_STORAGE_TASK_H

#include <coroutine>
#include <functional>
#include <optional>

#include "reduct/core/logger.h"

namespace reduct::async {

template <typename T>
struct Task {
 public:
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;

    promise_type() = default;

    Task get_return_object() { return Task{Handle::from_promise(*this)}; }

    std::suspend_never initial_suspend() {
      is_done_ = false;
      return {};
    }
    std::suspend_never final_suspend() noexcept {
      is_done_ = true;
      return {};
    }
    void return_value(T val) noexcept { value_ = val; }
    void unhandled_exception() { LOG_ERROR("Unhandled exception in coroutine"); }

    T value_;
    mutable std::atomic<bool> is_done_;
  };

  explicit Task(typename promise_type::Handle coro) : coro_(coro) {}

  std::optional<T> Get() {
    return coro_.promise().is_done_ ? std::make_optional(coro_.promise().value_) : std::nullopt;
  }

  template <typename R, typename P>
  inline std::optional<T> WaitFor(const std::chrono::duration<R, P>& time) {
    using Clock = std::chrono::steady_clock;
    auto start = Clock::now();

    std::optional<T> ret = Get();
    while (!ret && Clock::now() - start < time) {
      std::this_thread::sleep_for(kTick);
      ret = Get();
    }

    return ret;
  }

 private:
  typename promise_type::Handle coro_;
};

};      // namespace reduct::async
#endif  // REDUCT_STORAGE_TASK_H
