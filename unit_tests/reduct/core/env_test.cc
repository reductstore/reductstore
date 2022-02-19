// Copyright 2022 Alexey Timin

#include <catch2/catch.hpp>

#include "reduct/core/env_variable.h"

using reduct::core::EnvVariable;

TEST_CASE("core::Env can mask variable") {
  EnvVariable env;
  auto some_var = env.Get<std::string>("SOME_VAR", "default_value", true);

  REQUIRE(some_var == "default_value");
  REQUIRE("\tSOME_VAR = ********** (masked)\n" == env.Print());
}
