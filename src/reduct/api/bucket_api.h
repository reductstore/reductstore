// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_BUCKET_API_H
#define REDUCT_STORAGE_BUCKET_API_H

#include "reduct/api/common.h"
#include "reduct/auth/token_repository.h"
#include "reduct/storage/storage.h"

namespace reduct::api {

class BucketApi {
 public:
  /**
   * POST /b/:name
   */
  static core::Result<HttpRequestReceiver> CreateBucket(storage::IStorage* storage, std::string_view name);

  /**
   * GET /b/:name
   */
  static core::Result<HttpRequestReceiver> GetBucket(const storage::IStorage* storage, std::string_view name);

  /**
   * HEAD /b/:name
   */
  static core::Result<HttpRequestReceiver> HeadBucket(const storage::IStorage* storage, std::string_view name);

  /**
   * PUT /b/:name
   */
  static core::Result<HttpRequestReceiver> UpdateBucket(const storage::IStorage* storage, std::string_view name);

  /**
   * DELETE /b/:name
   */
  static core::Result<HttpRequestReceiver> RemoveBucket(storage::IStorage* storage, auth::ITokenRepository* repo,
                                                        std::string_view name);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_BUCKET_API_H
