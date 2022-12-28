// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/async/loop.h"

namespace reduct::async {

ILoop* ILoop::loop_ = nullptr;

void ILoop::set_loop(ILoop* new_loop) { loop_ = new_loop; }

ILoop& ILoop::loop() { return *loop_; }

}  // namespace reduct::async
