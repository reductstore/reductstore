// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/core/env_variable.h"

namespace reduct::core {

std::string EnvVariable::Print() { return stream_.str(); }

}  // namespace reduct::core
