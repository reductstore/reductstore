// Copyright 2022 Alexey Timin

#include "reduct/api/console.h"

#include <regex>

namespace reduct::api {

using core::Error;
using core::Result;

Result<HttpResponse> Console::UiRequest(const asset::IAssetManager* console, std::string_view base_path,
                                        std::string_view path) {
  static StringMap cache;

  std::string* content;

  auto it = cache.find(std::string(path));
  if (it != cache.end()) {
    content = &it->second;
  } else {
    auto ret = console->Read(path);
    switch (ret.error.code) {
      case 200:
        break;
      case 404: {
        // It's React.js paths
        ret = console->Read("index.html");
        break;
      }
      default: {
        return {{}, ret.error.code};
      }
    }

    auto [inserted_it, _] = cache.insert(
        {std::string(path), std::regex_replace(ret.result, std::regex("/ui/"), fmt::format("{}ui/", base_path))});
    content = &inserted_it->second;
  }

  return {
      HttpResponse{
          .content_length = content->size(),
          .input_call = [](auto chunk, bool last) { return Error::kOk; },
          .output_call =
              [content]() {
                return Result<std::string>{*content, Error::kOk};
              },
      },
      Error::kOk,
  };
}
}  // namespace reduct::api
