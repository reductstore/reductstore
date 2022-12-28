// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
