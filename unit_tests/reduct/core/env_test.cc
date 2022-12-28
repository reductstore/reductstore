// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include <catch2/catch.hpp>

#include "reduct/core/env_variable.h"

using reduct::core::EnvVariable;

TEST_CASE("core::Env can mask variable") {
  EnvVariable env;
  auto some_var = env.Get<std::string>("SOME_VAR", "default_value", true);

  REQUIRE(some_var == "default_value");
  REQUIRE("\tSOME_VAR = ********** (masked)\n" == env.Print());
}
