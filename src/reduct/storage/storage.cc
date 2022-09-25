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

  [[nodiscard]] Run<IQueryCallback::Result> OnQuery(const IQueryCallback::Request& req) const override {
    using Callback = IQueryCallback;

    return Run<Callback::Result>([this, req] {
      auto [entry, err] = GetOrCreateEntry(req.bucket_name, req.entry_name, true);
      if (err) {
        return Callback::Result{{}, err};
      }

      std::optional<Time> start_ts;
      if (!req.start_timestamp.empty()) {
        auto [ts, parse_err] = ParseTimestamp(req.start_timestamp, "start_timestamp");
        if (parse_err) {
          return Callback::Result{{}, parse_err};
        }
        start_ts = ts;
      }

      std::optional<Time> stop_ts;
      if (!req.stop_timestamp.empty()) {
        auto [ts, parse_err] = ParseTimestamp(req.stop_timestamp, "stop_timestamp");
        if (parse_err) {
          return Callback::Result{{}, parse_err};
        }
        stop_ts = ts;
      }

      std::chrono::seconds ttl{5};
      if (!req.ttl.empty()) {
        auto [val, parse_err] = ParseUInt(req.ttl, "ttl");
        if (parse_err) {
          return Callback::Result{{}, parse_err};
        }

        ttl = std::chrono::seconds(val);
      }

      auto [id, query_err] = entry->Query(start_ts, stop_ts, query::IQuery::Options{.ttl = ttl});

      Callback::Response resp;
      resp.set_id(id);
      return Callback::Result{resp, Error::kOk};
    });
  }

  [[nodiscard]] Run<INextCallback::Result> OnNextRecord(const INextCallback::Request& req) const override {
    using Callback = INextCallback;

    return Run<Callback::Result>([this, req]() {
      auto [entry, err] = GetOrCreateEntry(req.bucket_name, req.entry_name, true);
      if (err) {
        return Callback::Result{{}, err};
      }

      auto [id, parse_err] = ParseUInt(req.id, "id");
      if (parse_err) {
        return Callback::Result{{}, parse_err};
      }

      auto [next, start_err] = entry->Next(id);
      if (start_err) {
        return Callback::Result{{}, start_err};
      }

      return Callback::Result{{next.reader, next.last}, start_err};
    });
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

  [[nodiscard]] core::Result<IEntry::SPtr> GetOrCreateEntry(std::string_view bucket_name, std::string_view entry_name,
                                                            bool must_exist = false) const {
    auto [bucket_it, err] = FindBucket(bucket_name);
    if (err) {
      return {{}, err};
    }

    if (must_exist && !bucket_it->second->HasEntry(entry_name.data())) {
      return {{}, {.code = 404, .message = fmt::format("Entry '{}' could not be found", entry_name)}};
    }

    auto [entry, ref_error] = bucket_it->second->GetOrCreateEntry(entry_name.data());
    if (ref_error) {
      return {{}, ref_error};
    }

    auto entry_ptr = entry.lock();
    if (!entry_ptr) {
      return {{}, {.code = 500, .message = "Failed to resolve entry"}};
    }

    return {entry_ptr, Error::kOk};
  }

  static core::Result<Time> ParseTimestamp(std::string_view timestamp, std::string_view param_name = "ts") {
    auto ts = Time::clock::now();
    if (timestamp.empty()) {
      return {Time{}, Error{.code = 422, .message = fmt::format("'{}' parameter can't be empty", param_name)}};
    }
    try {
      ts = Time{} + std::chrono::microseconds(std::stoull(std::string{timestamp}));
      return {ts, Error::kOk};
    } catch (...) {
      return {Time{},
              Error{.code = 422,
                    .message = fmt::format("Failed to parse '{}' parameter: {} should unix times in microseconds",
                                           param_name, std::string{timestamp})}};
    }
  }

  static core::Result<uint64_t> ParseUInt(std::string_view timestamp, std::string_view param_name) {
    uint64_t val = 0;
    if (timestamp.empty()) {
      return {val, Error{.code = 422, .message = fmt::format("'{}' parameter can't be empty", param_name)}};
    }
    try {
      val = std::stoul(std::string{timestamp});
      return {val, Error::kOk};
    } catch (...) {
      return {val, Error{.code = 422,
                         .message = fmt::format("Failed to parse '{}' parameter: {} should be unsigned integer",
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
