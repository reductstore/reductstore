// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/storage.h"

#include <filesystem>
#include <utility>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/proto/api/bucket.pb.h"
#include "reduct/storage/bucket.h"

namespace reduct::storage {

using core::Error;
using core::Result;
using core::Time;

using proto::api::BucketInfo;
using proto::api::BucketInfoList;
using proto::api::FullBucketInfo;
using proto::api::ServerInfo;

namespace fs = std::filesystem;

class Storage : public IStorage {
 public:
  explicit Storage(Options options) : options_(std::move(options)), buckets_() {
    if (!fs::exists(options_.data_path)) {
      LOG_INFO("Folder '{}' doesn't exist. Create it.", options_.data_path.string());
      fs::create_directories(options_.data_path);
    }

    for (const auto& folder : fs::directory_iterator(options_.data_path)) {
      if (folder.is_directory()) {
        auto bucket_name = folder.path().filename().string();
        if (auto bucket = IBucket::Restore(folder)) {
          buckets_[bucket_name] = std::move(bucket);
        }
      }
    }

    LOG_INFO("Load {} buckets", buckets_.size());
    start_time_ = decltype(start_time_)::clock::now();
  }

  /**
   * Server API
   */
  [[nodiscard]] core::Result<ServerInfo> GetInfo() const override {
    using proto::api::BucketSettings;
    using proto::api::Defaults;

    size_t usage = 0;
    uint64_t oldest_ts = std::numeric_limits<uint64_t>::max();
    uint64_t latest_ts = 0;

    for (const auto& [_, bucket] : buckets_) {
      auto info = bucket->GetInfo();
      usage += info.size();
      oldest_ts = std::min(oldest_ts, info.oldest_record());
      latest_ts = std::max(latest_ts, info.latest_record());
    }

    ServerInfo info;
    info.set_version(kVersion);
    info.set_bucket_count(buckets_.size());
    info.set_uptime(
        std::chrono::duration_cast<std::chrono::seconds>(decltype(start_time_)::clock::now() - start_time_).count());
    info.set_usage(usage);
    info.set_oldest_record(oldest_ts);
    info.set_latest_record(latest_ts);

    *info.mutable_defaults()->mutable_bucket() = IBucket::GetDefaults();
    return {std::move(info), Error::kOk};
  }

  [[nodiscard]] core::Result<BucketInfoList> GetList() const override {
    BucketInfoList list;
    for (const auto& [name, bucket] : buckets_) {
      *list.add_buckets() = bucket->GetInfo();
    }
    return {std::move(list), Error::kOk};
  }
  /**
   * Bucket API
   */
  core::Error CreateBucket(const std::string& bucket_name, const proto::api::BucketSettings& settings) override {
    if (buckets_.find(bucket_name) != buckets_.end()) {
      return Error{.code = 409, .message = fmt::format("Bucket '{}' already exists", bucket_name)};
    }

    auto bucket = IBucket::Build(options_.data_path / bucket_name, settings);
    if (!bucket) {
      return Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")};
    }

    buckets_[bucket_name] = std::move(bucket);
    return Error::kOk;
  }

  core::Result<IBucket::WPtr> GetBucket(const std::string& bucket_name) const override {
    auto [bucket_it, err] = FindBucket(bucket_name);
    if (err) {
      return {{}, err};
    }

    return {bucket_it->second, err};
  }

  Error RemoveBucket(const std::string& bucket_name) override {
    auto [bucket_it, err] = FindBucket(bucket_name);
    if (err) {
      return err;
    }

    err = bucket_it->second->Clean();
    fs::remove(options_.data_path / bucket_name);
    buckets_.erase(bucket_it);

    return err;
  }

 private:
  using BucketMap = std::map<std::string, std::shared_ptr<IBucket>>;

  [[nodiscard]] std::pair<BucketMap::const_iterator, Error> FindBucket(std::string_view name) const {
    auto it = buckets_.find(std::string{name});
    if (it == buckets_.end()) {
      return {buckets_.end(), Error{.code = 404, .message = fmt::format("Bucket '{}' is not found", name)}};
    }

    return {it, Error::kOk};
  }

  Options options_;
  BucketMap buckets_;
  std::chrono::steady_clock::time_point start_time_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage
