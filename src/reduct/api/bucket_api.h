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
  static core::Result<HttpResponse> CreateBucket(storage::IStorage* storage, std::string_view bucket);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_BUCKET_API_H
