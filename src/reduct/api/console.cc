// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/console.h"

#include <regex>

namespace reduct::api {

using core::Error;
using core::Result;

Result<HttpRequestReceiver> Console::UiRequest(const rust::Box<rust_part::ZipAssetManager>& console,
                                               std::string_view base_path, std::string_view path) {
  static StringMap cache;

  std::string* content;

  auto it = cache.find(std::string(path));
  if (it != cache.end()) {
    content = &it->second;
  } else {
    rust::String ret;
    try {
      ret = console->read(rust::Str(path.data()));
    } catch (...) {
      // It's React.js paths
      try {
        ret = console->read("index.html");
      } catch (...) {
        return Error::InternalError("Can't read index.html from console");
      }
    }

    auto [inserted_it, _] = cache.insert(
        {std::string(path), std::regex_replace(ret.data(), std::regex("/ui/"), fmt::format("{}ui/", base_path))});
    content = &inserted_it->second;
  }

  return {
      [content](std::string_view chunk, bool last) -> Result<HttpResponse> {
        return Result<HttpResponse>{
            HttpResponse{
                .content_length = content->size(),
                .SendData =
                    [content]() {
                      return Result<std::string>{*content, Error::kOk};
                    },
            },
            Error::kOk,
        };
      },
      Error::kOk,
  };
}
}  // namespace reduct::api
