// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_TASK_H
#define REDUCT_STORAGE_TASK_H

#include <coroutine>
#include <functional>
#include <optional>

#include "reduct/async/loop.h"
#include "reduct/core/logger.h"

namespace reduct::async {

/**
 * Task with return value,
 * it destroys the promise object in destructor, so for define behaviour you should call Get() before the task
 * destructed
 * @tparam T - return type
 */
template <typename T>
struct Task {
 public:
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;

    promise_type() = default;
    ~promise_type() { LOG_TRACE("~promise_type"); }

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
  ~Task() {
    if (coro_) coro_.destroy();
  }

  /**
   * Resume the corutine if it is needed and return result
   * @return
   */
  T Get() const {
    if (!coro_.done()) coro_.resume();
    return std::move(coro_.promise().value_);
  }

 private:
  typename promise_type::Handle coro_;
};

/**
 * Simple task, promise_type is destroyed automatically
 */
struct VoidTask {
 public:
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;

    promise_type() = default;
    ~promise_type() { LOG_TRACE("~promise_type"); }

    VoidTask get_return_object() {
      LOG_TRACE("get_return_object");
      return {};
    }

    std::suspend_never initial_suspend() {
      LOG_TRACE("initial_suspend");
      return {};
    }
    std::suspend_never final_suspend() noexcept {
      LOG_TRACE("final_suspend");
      return {};
    }

    void return_void() noexcept { LOG_TRACE("return_void"); }
    void unhandled_exception() { LOG_ERROR("Unhandled exception in coroutine"); }
  };

  VoidTask() = default;
  ~VoidTask() = default;
};

}  // namespace reduct::async
#endif  // REDUCT_STORAGE_TASK_H
