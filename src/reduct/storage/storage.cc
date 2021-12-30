// Copyright 2021 Alexey Timin
#include "reduct/storage/storage.h"

#include <coroutine>
#include <filesystem>
#include <shared_mutex>
#include <utility>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/storage/bucket.h"

namespace reduct::storage {

using api::ICreateBucketCallback;
using api::IInfoCallback;
using async::Run;
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
  [[nodiscard]] Run<IInfoCallback::Result> OnInfo(const IInfoCallback::Request& res) const override {
    return Run<IInfoCallback::Result>([this] {
      api::IInfoCallback::Response resp;
      resp.version = reduct::kVersion;
      resp.bucket_number = buckets_.size();
      return IInfoCallback::Result{std::move(resp), Error{}};
    });
  }

  [[nodiscard]] Run<ICreateBucketCallback::Result> OnCreateBucket(const ICreateBucketCallback::Request& req) override {
    return Run<ICreateBucketCallback::Result>([this, req] {
      if (buckets_.find(req.name) != buckets_.end()) {
        return ICreateBucketCallback::Result{
            {}, Error{.code = 409, .message = fmt::format("Bucket '{}' already exists", req.name)}};
      }

      auto bucket = IBucket::Build({.name = req.name, .path = options_.data_path});
      if (!bucket) {
        auto err =Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")};
        return ICreateBucketCallback::Result{
            {}, Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")}};
      }

      buckets_[req.name] = std::move(bucket);
      return ICreateBucketCallback::Result{{}, Error{}};
    });
  }

 private:
  Options options_;
  std::map<std::string, std::unique_ptr<IBucket>> buckets_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage
