// Copyright 2021 Alexey Timin

#include <functional>
#include <future>

#include "reduct/async/loop.h"
#include "reduct/core/logger.h"
#ifndef REDUCT_STORAGE_EXECUTORS_H
#define REDUCT_STORAGE_EXECUTORS_H

namespace reduct::async {

template <typename T>
struct LoopExecutor {
  std::future<T> Commit(std::function<T()>&& task) {
    std::packaged_task<T()> wrapper([t = std::move(task)] { return t(); });
    std::future<T> future = wrapper.get_future();
    ILoop::loop().Defer([wrapper = std::move(wrapper)]() mutable { wrapper(); });
    return future;
  }
};

}  // namespace reduct::async
#endif  // REDUCT_STORAGE_EXECUTORS_H
