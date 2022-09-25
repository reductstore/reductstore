// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_BUCKET_API_H
#define REDUCT_STORAGE_BUCKET_API_H

#include "reduct/api/api.h"
#include "reduct/proto/api/bucket.pb.h"
#include "reduct/storage/storage.h"

namespace reduct::api {

class BucketApi {
 public:
  /**
   * POST /b/:name
   */
  static core::Result<HttpResponse> CreateBucket(storage::IStorage* storage, std::string_view name);

  /**
   * GET /b/:name
   */
  static core::Result<HttpResponse> GetBucket(const storage::IStorage* storage, std::string_view name);

  /**
   * HEAD /b/:name
   */
  static core::Result<HttpResponse> HeadBucket(const storage::IStorage* storage, std::string_view name);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_BUCKET_API_H
