// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/storage.h"

#include <algorithm>
#include <filesystem>
#include <shared_mutex>
#include <utility>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/proto/api/bucket.pb.h"
#include "reduct/storage/bucket.h"

namespace reduct::storage {

using async::Run;
using core::Error;

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
        auto bucket_name = folder.path().filename();
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
  [[nodiscard]] Run<IInfoCallback::Result> OnInfo(const IInfoCallback::Request& res) const override {
    using Callback = IInfoCallback;

    return Run<Callback::Result>([this] {
      using proto::api::ServerInfo;
      using Clk = IEntry::Time::clock;

      size_t usage = 0;
      uint64_t oldest_ts = Clk::to_time_t(IEntry::Time::clock::now());
      uint64_t latest_ts{};

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

      Callback::Response resp{.info = std::move(info)};
      return Callback::Result{std::move(resp), Error{}};
    });
  }

  [[nodiscard]] Run<IListStorageCallback::Result> OnStorageList(
      const IListStorageCallback::Request& req) const override {
    using Callback = IListStorageCallback;
    return Run<Callback::Result>([this] {
      using proto::api::BucketInfoList;
      using proto::api::BucketInfo;
      using Clk = IEntry::Time::clock;

      BucketInfoList list;
      for (const auto& [name, bucket] : buckets_) {
        *list.add_buckets() = bucket->GetInfo();
      }
      return Callback::Result{.result = {.buckets = std::move(list)}, .error = Error::kOk};
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

      auto bucket = IBucket::Build(options_.data_path / bucket_name, req.bucket_settings);
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

      const auto& bucket = *bucket_it->second;
      proto::api::FullBucketInfo info;
      info.mutable_info()->CopyFrom(bucket.GetInfo());
      info.mutable_settings()->CopyFrom(bucket.GetSettings());
      for (auto& entry : bucket.GetEntryList()) {
        info.add_entries(entry);
      }
      return Callback::Result{std::move(info), Error::kOk};
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

      err = bucket_it->second->SetSettings(std::move(req.new_settings));
      return Callback::Result{{}, Error::kOk};
    });
  }
  /**
   * Entry API
   */
  [[nodiscard]] IWriteEntryCallback::Result OnWriteEntry(const IWriteEntryCallback::Request& req) noexcept override {
    using Callback = IWriteEntryCallback;

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

    int64_t size;
    try {
      size = std::stol(req.content_length.data());
      if (size < 0) {
        return {{}, {.code = 411, .message = "negative content-length"}};
      }
    } catch (...) {
      return {{}, {.code = 411, .message = "bad or empty content-length"}};
    }

    auto [writer, writer_err] = entry.lock()->BeginWrite(ts, size);
    if (!writer_err) {
      auto quota_error = bucket_it->second->KeepQuota();
      if (quota_error) {
        LOG_ERROR("Didn't mange to keep quota: {}", quota_error.ToString());
      }
    }

    return Callback::Result{std::move(writer), writer_err};
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

      IEntry::Time ts;
      if (!req.latest) {
        auto [parsed_ts, parse_err] = ParseTimestamp(req.timestamp);
        if (parse_err) {
          return Callback::Result{{}, parse_err};
        }
        ts = parsed_ts;
      } else {
        ts = entry.lock()->GetInfo().latest_record_time;
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
      for (const auto& info : list) {
        auto rec = resp.add_records();
        rec->set_ts(std::chrono::duration_cast<std::chrono::microseconds>(info.time.time_since_epoch()).count());
        rec->set_size(info.size);
      }

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
  std::chrono::steady_clock::time_point start_time_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage
