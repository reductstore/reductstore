// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_ASYNC_RUN_H
#define REDUCT_ASYNC_RUN_H

#include <chrono>
#include <coroutine>
#include <future>

#include "reduct/async/executors.h"
#include "reduct/async/task.h"

namespace reduct::async {

/**
 * Push a task to an executor
 * by default use LoopExecutor so it defers the tasks thread safely
 * @tparam T
 * @tparam Executor
 */
template <typename T, typename Executor = LoopExecutor<T>>
struct Run {
  using Us = std::chrono::microseconds;

  Run(std::function<T()>&& task, Executor* executor) { task_ = executor->Commit(std::move(task)); }
  explicit Run(std::function<T()>&& task) { task_ = LoopExecutor<T>().Commit(std::move(task)); }

  bool await_ready() const noexcept { return CheckTask(); }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    if (CheckTask()) {
      LOG_TRACE("Resume {}", *(int*)h.address());
      h.resume();
    } else {
      LOG_TRACE("Deffer {}", *(int*)h.address());
      ILoop::loop().Defer([this, h] { await_suspend(h); });
    }
  }

  T await_resume() noexcept { return task_.get(); }

 private:
  Run() = default;
  inline bool CheckTask() const { return task_.wait_for(Us(10)) == std::future_status::ready; }
  std::future<T> task_;
};

/**
 * Push a periodical task to an executor
 * by default use LoopExecutor so it defers the tasks thread safely
 * @tparam T
 * @tparam Executor
 */
template <typename T, typename Executor = LoopExecutor<std::optional<T>>>
struct RunUntil {
  using Us = std::chrono::microseconds;

  RunUntil(std::function<std::optional<T>()>&& task, Executor* executor) : executor_(executor), func_(std::move(task)) {
    task_ = executor_->Commit(func_);
  }

  explicit RunUntil(std::function<std::optional<T>()>&& task) : executor_{}, func_(std::move(task)) {
    task_ = LoopExecutor<std::optional<T>>().Commit(func_);
  }

  constexpr bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> h) noexcept {
    if (CheckTask()) {
      LOG_TRACE("Resume {}", *(int*)h.address());
      result_ = task_.get();
      if (result_) {
        h.resume();
      } else {
        // if result is nullopt, repeat task
        if (executor_ != nullptr) {
          task_ = executor_->Commit(func_);
        } else {
          task_ = LoopExecutor<std::optional<T>>().Commit(func_);
        }

        ILoop::loop().Defer([this, h] { await_suspend(h); });
      }

    } else {
      LOG_TRACE("Deffer {}", *(int*)h.address());
      ILoop::loop().Defer([this, h] { await_suspend(h); });
    }
  }

  T await_resume() noexcept { return *result_; }

 private:
  RunUntil() = default;
  inline bool CheckTask() const { return task_.wait_for(Us(10)) == std::future_status::ready; }

  std::future<std::optional<T>> task_;
  std::optional<T> result_;
  Executor* executor_;
  std::function<std::optional<T>()> func_;
};

}  // namespace reduct::async
#endif  // REDUCT_ASYNC_RUN_H
