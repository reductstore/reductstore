// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
    return err;
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
    return err;
  }

  return DefaultReceiver();
}

core::Result<HttpRequestReceiver> BucketApi::UpdateBucket(const storage::IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return err;
  }

  return ReceiveJson<BucketSettings>(
      [bucket = bucket_ptr.lock()](auto settings) { return bucket->SetSettings(settings); });
}
core::Result<HttpRequestReceiver> BucketApi::RemoveBucket(storage::IStorage* storage,
                                                          reduct::rust_part::TokenRepository& repo,
                                                          std::string_view name) {
  if (auto err = storage->RemoveBucket(std::string(name))) {
    return err;
  }

  // Remove bucket from all tokens
  auto err = reduct::rust_part::token_repo_remove_bucket_from_tokens(repo, name.data());
  if (err->status() != Error::kOk.code) {
    return Error(err->status(), err->message().data());
  }

  return DefaultReceiver();
}

}  // namespace reduct::api
