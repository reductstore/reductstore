// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_HELPERS_H
#define REDUCT_STORAGE_HELPERS_H

#include <fmt/core.h>

#include <filesystem>
#include <random>

#include "reduct/storage/storage.h"

inline bool operator==(const google::protobuf::MessageLite& msg_a, const google::protobuf::MessageLite& msg_b) {
  return (msg_a.GetTypeName() == msg_b.GetTypeName()) && (msg_a.SerializeAsString() == msg_b.SerializeAsString());
}

/**
 * Build a directory in /tmp with random name
 * @return
 */
inline std::filesystem::path BuildTmpDirectory() {
  std::random_device r;

  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(1, 10000000);

  std::filesystem::path path = std::filesystem::temp_directory_path() / fmt::format("reduct_{}", uniform_dist(e1));
  std::filesystem::create_directories(path);
  return path;
}

namespace reduct {

static const proto::api::BucketSettings MakeDefaultBucketSettings() {
  using proto::api::BucketSettings;

  BucketSettings settings;
  settings.set_max_block_size(1000);
  settings.set_quota_type(BucketSettings::NONE);
  settings.set_quota_size(10);

  return settings;
}

inline async::Task<api::IInfoCallback::Result> OnInfo(storage::IStorage* storage) {
  auto result = co_await storage->OnInfo({});
  co_return result;
}

inline async::Task<api::IListStorageCallback::Result> OnStorageList(storage::IStorage* storage) {
  auto result = co_await storage->OnStorageList({});
  co_return result;
}

inline async::Task<api::ICreateBucketCallback::Result> OnCreateBucket(storage::IStorage* storage,
                                                                      api::ICreateBucketCallback::Request req) {
  auto result = co_await storage->OnCreateBucket(std::move(req));
  co_return result;
}

inline async::Task<api::IGetBucketCallback::Result> OnGetBucket(storage::IStorage* storage,
                                                                api::IGetBucketCallback::Request req) {
  auto result = co_await storage->OnGetBucket(std::move(req));
  co_return result;
}

inline async::Task<api::IUpdateBucketCallback::Result> OnChangeBucketSettings(storage::IStorage* storage,
                                                                              api::IUpdateBucketCallback::Request req) {
  auto result = co_await storage->OnUpdateCallback(std::move(req));
  co_return result;
}

inline async::Task<api::IRemoveBucketCallback::Result> OnRemoveBucket(storage::IStorage* storage,
                                                                      api::IRemoveBucketCallback::Request req) {
  auto result = co_await storage->OnRemoveBucket(std::move(req));
  co_return result;
}

inline async::Task<api::IWriteEntryCallback::Result> OnWriteEntry(storage::IStorage* storage,
                                                                  api::IWriteEntryCallback::Request req) {
  auto result = co_await storage->OnWriteEntry(std::move(req));
  co_return result;
}

inline async::Task<api::IReadEntryCallback::Result> OnReadEntry(storage::IStorage* storage,
                                                                api::IReadEntryCallback::Request req) {
  auto result = co_await storage->OnReadEntry(std::move(req));
  co_return result;
}

inline async::Task<api::IListEntryCallback::Result> OnListEntry(storage::IStorage* storage,
                                                                api::IListEntryCallback::Request req) {
  auto result = co_await storage->OnListEntry(std::move(req));
  co_return result;
}
}  // namespace reduct

#endif  // REDUCT_STORAGE_HELPERS_H
