// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_TASK_H
#define REDUCT_STORAGE_TASK_H

#include <coroutine>
#include <functional>
#include <optional>

#include "reduct/async/loop.h"
#include "reduct/core/logger.h"

namespace reduct::async {

template <typename T>
struct Task {
 public:
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;

    promise_type() = default;
    ~promise_type() { LOG_TRACE("~promise_type"); };

    Task get_return_object() {
      LOG_TRACE("get_return_object");
      return Task{Handle::from_promise(*this)};
    }

    std::suspend_never initial_suspend() {
      LOG_TRACE("initial_suspend");
      return {};
    }
    std::suspend_always final_suspend() noexcept {
      LOG_TRACE("final_suspend");
      return {};
    }

    void return_value(T val) noexcept {
      LOG_TRACE("return_value");
      value_ = std::move(val);
    }
    void unhandled_exception() { LOG_ERROR("Unhandled exception in coroutine"); }

    T value_;
  };

  explicit Task(typename promise_type::Handle coro) : coro_(coro) {}
  ~Task() { coro_.destroy(); }

  T Get() {
    LOG_TRACE("Get");
    if (!coro_.done()) coro_.resume();
    return coro_.promise().value_;
  }

 private:
  typename promise_type::Handle coro_;
};

};      // namespace reduct::async
#endif  // REDUCT_STORAGE_TASK_H
