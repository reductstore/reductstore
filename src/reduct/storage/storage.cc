// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/storage.h"

#include <algorithm>
#include <filesystem>
#include <numeric>
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
        if (auto bucket = IBucket::Restore(folder)) {
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
      resp.bucket_count = buckets_.size();
      resp.entry_count = std::accumulate(buckets_.begin(), buckets_.end(), 0,
                                         [](size_t a, const decltype(buckets_)::value_type& entry) {
                                           return entry.second->GetInfo().entry_count + a;
                                         });
      return Callback::Result{std::move(resp), Error{}};
    });
  }

  /**
   * Bucket API
   */
  [[nodiscard]] Run<ICreateBucketCallback::Result> OnCreateBucket(const ICreateBucketCallback::Request& req) override {
    using Callback = ICreateBucketCallback;
    return Run<Callback::Result>([this, req] {
      std::string bucket_name{req.bucket_name};
      if (buckets_.find(bucket_name) != buckets_.end()) {
        return Callback::Result{
            {}, Error{.code = 409, .message = fmt::format("Bucket '{}' already exists", req.bucket_name)}};
      }

      auto [quota_type, parse_err] = IBucket::ParseQuotaType(req.bucket_settings.quota_type);
      if (parse_err) {
        return Callback::Result{{}, parse_err};
      }

      auto bucket = IBucket::Build({
          .name = bucket_name,
          .path = options_.data_path,
          .max_block_size = req.bucket_settings.max_block_size,
          .quota = {.type = quota_type, .size = req.bucket_settings.quota_size},
      });
      if (!bucket) {
        auto err = Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")};
        return Callback::Result{{},
                                Error{.code = 500, .message = fmt::format("Internal error: Failed to create bucket")}};
      }

      buckets_[bucket_name] = std::move(bucket);
      return Callback::Result{{}, Error{}};
    });
  }

  [[nodiscard]] Run<IGetBucketCallback::Result> OnGetBucket(const IGetBucketCallback::Request& req) const override {
    using Callback = IGetBucketCallback;
    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.bucket_name);
      if (err) {
        return Callback::Result{{}, err};
      }

      const auto& settings = bucket_it->second->GetOptions();
      return Callback::Result{{.bucket_settings = {.max_block_size = settings.max_block_size,
                                                   .quota_type = IBucket::GetQuotaTypeName(settings.quota.type),
                                                   .quota_size = settings.quota.size}},
                              Error{}};
    });
  }

  [[nodiscard]] Run<IRemoveBucketCallback::Result> OnRemoveBucket(const IRemoveBucketCallback::Request& req) override {
    using Callback = IRemoveBucketCallback;

    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.bucket_name);
      if (err) {
        return Callback::Result{{}, err};
      }

      err = bucket_it->second->Clean();
      fs::remove(options_.data_path / req.bucket_name);
      buckets_.erase(bucket_it);

      return Callback::Result{{}, err};
    });
  }

  Run<IUpdateBucketCallback::Result> OnUpdateCallback(const IUpdateBucketCallback::Request& req) override {
    using Callback = IUpdateBucketCallback;
    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.bucket_name);
      if (err) {
        return Callback::Result{{}, err};
      }

      auto [quota_type, parse_err] = IBucket::ParseQuotaType(req.new_settings.quota_type);
      if (parse_err) {
        return Callback::Result{{}, parse_err};
      }

      err = bucket_it->second->SetOptions({
          .name = "",
          .path = {},
          .max_block_size = req.new_settings.max_block_size,
          .quota = {.type = quota_type, .size = req.new_settings.quota_size},
      });
      return Callback::Result{{}, Error::kOk};
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

      auto [ts, parse_err] = ParseTimestamp(req.timestamp);
      if (parse_err) {
        return Callback::Result{{}, parse_err};
      }

      auto [entry, ref_error] = bucket_it->second->GetOrCreateEntry(std::string(req.entry_name));
      if (ref_error) {
        return Callback::Result{{}, ref_error};
      }

      err = entry.lock()->Write(req.blob, ts);
      if (!err) {
        auto quota_error = bucket_it->second->KeepQuota();
        if (quota_error) {
          LOG_ERROR("Didn't mange to keep quota: {}", quota_error.ToString());
        }
      }

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

      auto [entry, ref_error] = bucket_it->second->GetOrCreateEntry(std::string(req.entry_name));
      if (ref_error) {
        return Callback::Result{{}, ref_error};
      }

      auto [ts, parse_err] = ParseTimestamp(req.timestamp);
      if (parse_err) {
        return Callback::Result{{}, parse_err};
      }

      auto read_result = entry.lock()->Read(ts);
      return Callback::Result{
          {
              .blob = read_result.blob,
              .timestamp = std::to_string(
                  std::chrono::duration_cast<std::chrono::microseconds>(read_result.time.time_since_epoch()).count()),
          },
          read_result.error,
      };
    });
  }

  [[nodiscard]] Run<IListEntryCallback::Result> OnListEntry(const IListEntryCallback::Request& req) const override {
    using Callback = IListEntryCallback;

    return Run<Callback::Result>([this, req] {
      auto [bucket_it, err] = FindBucket(req.bucket_name);
      if (err) {
        return Callback::Result{{}, err};
      }

      auto [entry, ref_error] = bucket_it->second->GetOrCreateEntry(std::string(req.entry_name));
      if (ref_error) {
        return Callback::Result{{}, ref_error};
      }

      auto [start_ts, parse_start_err] = ParseTimestamp(req.start_timestamp, "start_timestamp");
      if (parse_start_err) {
        return Callback::Result{{}, parse_start_err};
      }

      auto [stop_ts, parse_stop_err] = ParseTimestamp(req.stop_timestamp, "stop_timestamp");
      if (parse_stop_err) {
        return Callback::Result{{}, parse_stop_err};
      }

      auto [list, list_err] = entry.lock()->List(start_ts, stop_ts);
      if (list_err) {
        return Callback::Result{{}, list_err};
      }

      Callback::Response resp;
      std::ranges::transform(list, std::back_inserter(resp.records), [](const IEntry::RecordInfo& info) {
        return Callback::RecordInfo{
            .timestamp = std::chrono::duration_cast<std::chrono::microseconds>(info.time.time_since_epoch()).count(),
            .size = info.size};
      });

      return Callback::Result{std::move(resp), {}};
    });
  }

 private:
  using BucketMap = std::map<std::string, std::unique_ptr<IBucket>>;

  [[nodiscard]] std::pair<BucketMap::const_iterator, Error> FindBucket(std::string_view name) const {
    auto it = buckets_.find(std::string{name});
    if (it == buckets_.end()) {
      return {buckets_.end(), Error{.code = 404, .message = fmt::format("Bucket '{}' is not found", name)}};
    }

    return {it, Error::kOk};
  }

  static std::tuple<IEntry::Time, Error> ParseTimestamp(std::string_view timestamp,
                                                        std::string_view param_name = "ts") {
    auto ts = IEntry::Time::clock::now();
    if (timestamp.empty()) {
      return {IEntry::Time{}, Error{.code = 422, .message = fmt::format("'{}' parameter can't be empty", param_name)}};
    }
    try {
      ts = IEntry::Time{} + std::chrono::microseconds(std::stol(std::string{timestamp}));
      return {ts, Error::kOk};
    } catch (...) {
      return {IEntry::Time{},
              Error{.code = 422,
                    .message = fmt::format("Failed to parse '{}' parameter: {} should unix times in microseconds",
                                           param_name, std::string{timestamp})}};
    }
  }

  Options options_;
  BucketMap buckets_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage
