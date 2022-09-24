// Copyright 2022 Alexey Timin

#include "reduct/api/bucket_api.h"

namespace reduct::api {

using core::Result;
using storage::IStorage;
using proto::api::BucketSettings;

Result<HttpResponse> BucketApi::CreateBucket(IStorage* storage, std::string_view bucket) {
  return ReceiveJson<proto::api::BucketSettings>(
      [storage, bucket = std::string(bucket)](auto settings) { return storage->CreateBucket(bucket, settings); });
}

}  // namespace reduct::api
