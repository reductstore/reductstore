// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/common.h"

#include <catch2/catch.hpp>

using reduct::api::ParseQueryString;

TEST_CASE("ParseQueryString parse query") {
  SECTION("empty") { REQUIRE(ParseQueryString("").empty()); }
  SECTION("one") { REQUIRE(ParseQueryString("a=b") == std::map<std::string, std::string>({{"a", "b"}})); }
  SECTION("two") {
    REQUIRE(ParseQueryString("a=b&c=d") == std::map<std::string, std::string>({{"a", "b"}, {"c", "d"}}));
  }
  SECTION("three") {
    REQUIRE(ParseQueryString("a=b&c=d&e=f") ==
            std::map<std::string, std::string>({{"a", "b"}, {"c", "d"}, {"e", "f"}}));
  }
  SECTION("three with empty") {
    REQUIRE(ParseQueryString("a=b&c=d&e=") == std::map<std::string, std::string>({{"a", "b"}, {"c", "d"}, {"e", ""}}));
  }
  SECTION("three with empty key") {
    REQUIRE(ParseQueryString("a=b&=d&e=f") == std::map<std::string, std::string>({{"a", "b"}, {"", "d"}, {"e", "f"}}));
  }
}
