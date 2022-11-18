// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_BUCKET_API_H
#define REDUCT_STORAGE_BUCKET_API_H

#include "reduct/api/common.h"
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
  static core::Result<HttpRequestReceiver> RemoveBucket(storage::IStorage* storage, std::string_view name);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_BUCKET_API_H
