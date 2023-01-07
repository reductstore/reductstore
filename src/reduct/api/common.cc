// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/api/common.h"

namespace reduct::api {

std::map<std::string, std::string> ParseQueryString(std::string_view query) {
  std::map<std::string, std::string> result;
  std::string key;
  std::string value;
  bool is_key = true;
  for (auto c : query) {
    if (c == '=') {
      is_key = false;
    } else if (c == '&') {
      result[key] = value;
      key.clear();
      value.clear();
      is_key = true;
    } else {
      if (is_key) {
        key += c;
      } else {
        value += c;
      }
    }
  }

  if (!key.empty()) {
    result[key] = value;
  }

  return result;
}

}  // namespace reduct::api
