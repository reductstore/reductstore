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
    err.message = "";
  }

  return {
      [err = std::move(err)](std::string_view chunk, bool last) -> core::Result<HttpResponse> {
        return {HttpResponse::Default(), err};
      },
      core::Error::kOk,
  };
}

core::Result<HttpRequestReceiver> BucketApi::UpdateBucket(const storage::IStorage* storage, std::string_view name) {
  auto [bucket_ptr, err] = storage->GetBucket(std::string(name));
  if (err) {
    return err;
  }

  return ReceiveJson<BucketSettings>(
      [bucket = bucket_ptr.lock()](auto settings) { return bucket->SetSettings(settings); });
}
core::Result<HttpRequestReceiver> BucketApi::RemoveBucket(storage::IStorage* storage, auth::ITokenRepository* repo,
                                                          std::string_view name) {
  if (auto err = storage->RemoveBucket(std::string(name))) {
    return err;
  }

  // Remove bucket from all tokens
  const auto tokens = repo->GetTokenList();
  if (tokens.error) {
    return tokens.error;
  }

  for (const auto& token : tokens.result) {
    const auto [token_info, err] = repo->FindByName(token.name());
    if (err) {
      return err;
    }

    proto::api::Token_Permissions permissions;
    permissions.set_full_access(token_info.permissions().full_access());

    for (const auto& bucket : token_info.permissions().read()) {
      if (bucket != name) {
        *permissions.add_read() = bucket;
      }
    }

    for (const auto& bucket : token_info.permissions().write()) {
      if (bucket != name) {
        *permissions.add_write() = bucket;
      }
    }

    if (auto update_err = repo->UpdateToken(token.name(), permissions)) {
      return update_err;
    }
  }

  return DefaultReceiver();
}

}  // namespace reduct::api
