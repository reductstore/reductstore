// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <functional>
#include <future>

#include "reduct/async/loop.h"
#include "reduct/core/logger.h"
#ifndef REDUCT_STORAGE_EXECUTORS_H
#define REDUCT_STORAGE_EXECUTORS_H

namespace reduct::async {

/**
 * Thread safe executor in the main event loop
 * @tparam T
 */
template <typename T>
struct LoopExecutor {
  std::future<T> Commit(std::function<T()> task) {
    std::packaged_task<T()> wrapper([t = std::move(task)] { return t(); });
    std::future<T> future = wrapper.get_future();
    ILoop::loop().Defer([wrapper = std::move(wrapper)]() mutable { wrapper(); });
    return future;
  }
};

}  // namespace reduct::async
#endif  // REDUCT_STORAGE_EXECUTORS_H
