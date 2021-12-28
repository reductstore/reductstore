// Copyright 2021 Alexey Timin
#include "reduct/storage/storage.h"

#include <filesystem>
#include <utility>
#include <shared_mutex>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/storage/bucket.h"

namespace reduct::storage {

using core::Error;
namespace fs = std::filesystem;

class Storage : public IStorage {
 public:
  explicit Storage(Options options) : options_(std::move(options)), buckets_() {
    for (const auto& folder : fs::directory_iterator(options_.data_path)) {
      if (folder.is_directory()) {
        auto bucket_name = folder.path().filename();
        if (auto bucket = IBucket::Build({.name = bucket_name, .path = options_.data_path})) {
          buckets_[bucket_name] = std::move(bucket);
        }
      }
    }

    LOG_INFO("Load {} buckets", buckets_.size());
  }

  /**
   * API Implementation
   */
  Error OnInfo(api::IInfoCallback::Response* info, const api::IInfoCallback::Request& res) const override {
    info->version = reduct::kVersion;
    {
      std::shared_lock lock(mutex_);
      info->bucket_number = buckets_.size();
    }
    return {};
  }

  Error OnCreateBucket(api::ICreateBucketCallback::Response* res,
                       const api::ICreateBucketCallback::Request& req) override {
    if (buckets_.find(req.name) != buckets_.end()) {
      return Error{.code = 409, .message = fmt::format("Bucket '{}' already exists", req.name)};
    }

    auto bucket = IBucket::Build({.name = req.name, .path = options_.data_path});
    if (!bucket) {
      return Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")};
    }

    {
      std::unique_lock lock(mutex_);
      buckets_[req.name] = std::move(bucket);
    }
    return {};
  }

 private:
  Options options_;
  std::map<std::string, std::unique_ptr<IBucket>> buckets_;
  mutable std::shared_mutex mutex_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage
