// Copyright 2022 Alexey Timin

#include "reduct/api/bucket_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using proto::api::BucketSettings;
using proto::api::FullBucketInfo;
using storage::IStorage;

Result<HttpRequestReceiver> BucketApi::CreateBucket(IStorage* storage, std::string_view name) {
  return ReceiveJson<BucketSettings>([storage, bucket = std::string(name)](BucketSettings&& settings) -> Error {
    return storage->CreateBucket(bucket, settings);
  });
}

Result<HttpRequestReceiver> BucketApi::GetBucket(const IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return DefaultReceiver(err);
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

core::Result<HttpRequestReceiver> BucketApi::HeadBucket(const storage::IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    err.message = "";
  }

  return DefaultReceiver(err);
}

core::Result<HttpRequestReceiver> BucketApi::UpdateBucket(const storage::IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return DefaultReceiver(err);
  }

  return ReceiveJson<BucketSettings>(
      [bucket = bucket_ptr.lock()](auto settings) { return bucket->SetSettings(settings); });
}
core::Result<HttpRequestReceiver> BucketApi::RemoveBucket(storage::IStorage* storage, std::string_view name) {
  return DefaultReceiver(storage->RemoveBucket(std::string(name)));
}

}  // namespace reduct::api
