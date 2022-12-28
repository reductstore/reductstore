// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_LOOP_H
#define REDUCT_STORAGE_LOOP_H

#include <uWebSockets/MoveOnlyFunction.h>

#include <chrono>
#include <functional>
namespace reduct::async {

static const std::chrono::microseconds kTick{10};

class ILoop {
 public:
  using Task = uWS::MoveOnlyFunction<void()>;

  virtual void Defer(Task&& task) = 0;

  static void set_loop(ILoop*);
  static ILoop& loop();

 private:
  static ILoop* loop_;
};
}  // namespace reduct::async
#endif  // REDUCT_STORAGE_LOOP_H
