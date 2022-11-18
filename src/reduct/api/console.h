// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_CONSOLE_H
#define REDUCT_STORAGE_CONSOLE_H

#include "reduct/api/common.h"
#include "reduct/asset/asset_manager.h"

namespace reduct::api {

class Console {
 public:
  static core::Result<HttpRequestReceiver> UiRequest(const asset::IAssetManager* console, std::string_view base_path,
                                              std::string_view path);
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_CONSOLE_H
