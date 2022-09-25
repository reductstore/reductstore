// Copyright 2022 Alexey Timin

#include "reduct/api/bucket_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using proto::api::BucketSettings;
using proto::api::FullBucketInfo;
using storage::IStorage;

Result<HttpResponse> BucketApi::CreateBucket(IStorage* storage, std::string_view name) {
  return ReceiveJson<BucketSettings>(
      [storage, bucket = std::string(name)](auto settings) { return storage->CreateBucket(bucket, settings); });
}

Result<HttpResponse> BucketApi::GetBucket(const IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return {{}, err};
  }

  auto bucket = bucket_ptr.lock();
  FullBucketInfo info;

  info.mutable_info()->CopyFrom(bucket->GetInfo());
  info.mutable_settings()->CopyFrom(bucket->GetSettings());
  for (const auto& entry : bucket->GetEntryList()) {
    *info.add_entries() = entry;
  }

  return SendJson<FullBucketInfo>({std::move(info), Error::kOk});
}

core::Result<HttpResponse> BucketApi::HeadBucket(const storage::IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return {{}, err};
  }

  return {HttpResponse::Default(), Error::kOk};
}

core::Result<HttpResponse> BucketApi::UpdateBucket(const storage::IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return {{}, err};
  }

  return ReceiveJson<BucketSettings>(
      [bucket = bucket_ptr.lock()](auto settings) { return bucket->SetSettings(settings); });
}
core::Result<HttpResponse> BucketApi::RemoveBucket(storage::IStorage* storage, std::string_view name) {
  if (auto err = storage->RemoveBucket(std::string(name))) {
    return {{}, err};
  }

  return {HttpResponse::Default(), Error::kOk};
}

}  // namespace reduct::api
