// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_SERVER_API_H
#define REDUCT_STORAGE_SERVER_API_H

#include "reduct/api/common.h"
#include "reduct/storage/storage.h"
#include "reduct/auth/token_repository.h"

namespace reduct::api {

class ServerApi {
 public:
  /**
   * GET /alive
   */
  static core::Result<HttpRequestReceiver> Alive(const storage::IStorage* storage);

  /**
   * GET /info
   */
  static core::Result<HttpRequestReceiver> Info(const storage::IStorage* storage);

  /**
   * GET /list
   */
  static core::Result<HttpRequestReceiver> List(const storage::IStorage* storage);

  /**
   * GET /me
   */
  static core::Result<HttpRequestReceiver> Me(const auth::ITokenRepository* token_repo, std::string_view auth_header);
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_SERVER_API_H
