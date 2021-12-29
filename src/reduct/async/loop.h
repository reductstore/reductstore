// Copyright 2021 Alexey Timin

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
  void Defer(std::function<void()>& task) = delete;

  static void set_loop(ILoop*);
  static ILoop& loop();

 private:
  static ILoop* loop_;
};
}  // namespace reduct::async
#endif  // REDUCT_STORAGE_LOOP_H
