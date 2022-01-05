// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/storage.h"

#include <coroutine>
#include <filesystem>
#include <shared_mutex>
#include <utility>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/storage/bucket.h"

namespace reduct::storage {

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
   * Info API
   */
  [[nodiscard]] Run<IInfoCallback::Result> OnInfo(const IInfoCallback::Request& res) const override {
    using Callback = IInfoCallback;
    return Run<Callback::Result>([this] {
      Callback::Response resp;
      resp.version = reduct::kVersion;
      resp.bucket_number = buckets_.size();
      return Callback::Result{std::move(resp), Error{}};
    });
  }

  /**
   * Bucket API
   */
  [[nodiscard]] Run<ICreateBucketCallback::Result> OnCreateBucket(const ICreateBucketCallback::Request& req) override {
    using Callback = ICreateBucketCallback;
    return Run<Callback::Result>([this, req] {
      if (buckets_.find(req.name) != buckets_.end()) {
        return Callback::Result{{}, Error{.code = 409, .message = fmt::format("Bucket '{}' already exists", req.name)}};
      }

      auto bucket = IBucket::Build({.name = req.name, .path = options_.data_path});
      if (!bucket) {
        auto err = Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")};
        return Callback::Result{{},
                                Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")}};
      }

      buckets_[req.name] = std::move(bucket);
      return Callback::Result{{}, Error{}};
    });
  }

  [[nodiscard]] Run<IGetBucketCallback::Result> OnGetBucket(const IGetBucketCallback::Request& req) const override {
    using Callback = IGetBucketCallback;
    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.name);
      if (err) {
        return Callback::Result{{}, err};
      }

      return Callback::Result{{}, Error{}};
    });
  }

  [[nodiscard]] Run<IRemoveBucketCallback::Result> OnRemoveBucket(const IRemoveBucketCallback::Request& req) override {
    using Callback = IRemoveBucketCallback;

    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.name);
      if (err) {
        return Callback::Result{{}, err};
      }

      err = bucket_it->second->Clean();
      fs::remove(options_.data_path / req.name);
      buckets_.erase(bucket_it);

      return Callback::Result{{}, err};
    });
  }

  /**
   * Entry API
   */
  [[nodiscard]] Run<IWriteEntryCallback::Result> OnWriteEntry(const IWriteEntryCallback::Request& req) override {
    using Callback = IWriteEntryCallback;

    return Run<Callback::Result>([this, &req]() mutable {
      auto [bucket_it, err] = FindBucket(req.bucket_name);
      if (err) {
        return Callback::Result{{}, err};
      }

      auto ret = bucket_it->second->GetOrCreateEntry(req.entry_name);
      if (ret.error) {
        return Callback::Result{
            {}, Error{.code = 500, .message = fmt::format("Failed to create entry {}", req.entry_name)}};
      }

      err = ret.entry.lock()->Write(req.blob, IEntry::Time() + std::chrono::microseconds(req.timestamp));
      return Callback::Result{{}, err};
    });
  }

  Run<IReadEntryCallback::Result> OnReadEntry(const IReadEntryCallback::Request& req) override {
    using Callback = IReadEntryCallback;

    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.bucket_name);
      if (err) {
        return Callback::Result{{}, err};
      }

      auto ret = bucket_it->second->GetOrCreateEntry(req.entry_name);
      if (ret.error) {
        return Callback::Result{
            {}, Error{.code = 500, .message = fmt::format("Failed to create entry {}", req.entry_name)}};
      }

      auto read_result = ret.entry.lock()->Read(IEntry::Time() + std::chrono::microseconds(req.timestamp));
      return Callback::Result{
          {
              .blob = read_result.blob,
              .timestamp =
                  std::chrono::duration_cast<std::chrono::microseconds>(read_result.time.time_since_epoch()).count(),
          },
          read_result.error,
      };
    });
  }

 private:
  using BucketMap = std::map<std::string, std::unique_ptr<IBucket>>;

  [[nodiscard]] std::pair<BucketMap::const_iterator, Error> FindBucket(const std::string& name) const {
    auto it = buckets_.find(name);
    if (buckets_.find(name) == buckets_.end()) {
      return {buckets_.end(), Error{.code = 404, .message = fmt::format("Bucket '{}' is not found", name)}};
    }

    return {it, Error::kOk};
  }

  Options options_;
  BucketMap buckets_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage
