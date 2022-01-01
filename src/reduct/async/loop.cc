// Copyright 2021-2022 Alexey Timin

#include "reduct/async/loop.h"

namespace reduct::async {

ILoop* ILoop::loop_ = nullptr;

void ILoop::set_loop(ILoop* new_loop) { loop_ = new_loop; }

ILoop& ILoop::loop() { return *loop_; }

}  // namespace reduct::async
