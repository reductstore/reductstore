// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/bucket.h"

#include <google/protobuf/util/time_util.h>

#include <fstream>
#include <numeric>
#include <ranges>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/proto/api/bucket.pb.h"

namespace reduct::storage {

namespace fs = std::filesystem;
using core::Error;
using proto::api::BucketInfo;
using proto::api::BucketSettings;
using proto::api::EntryInfo;

using google::protobuf::util::TimeUtil;

class Bucket : public IBucket {
 public:
  explicit Bucket(fs::path full_path, BucketSettings settings)
      : full_path_(std::move(full_path)),
        name_(full_path_.filename().string()),
        settings_(std::move(settings)),
        entry_map_() {
    if (fs::exists(full_path_)) {
      throw std::runtime_error(fmt::format("Path '{}' already exists", full_path_.string()));
    }

    const auto& default_settings = Bucket::GetDefaults();
    if (!settings_.has_max_block_size()) {
      settings_.set_max_block_size(default_settings.max_block_size());
    }

    if (!settings_.has_quota_type()) {
      settings_.set_quota_type(default_settings.quota_type());
    }

    if (!settings_.has_quota_size()) {
      settings_.set_quota_size(default_settings.quota_size());
    }

    if (!settings_.has_max_block_records()) {
      settings_.set_max_block_records(default_settings.max_block_records());
    }

    fs::create_directories(full_path_);
    auto err = SaveDescriptor();
    if (err) {
      throw std::runtime_error(err.message);
    }
  }

  explicit Bucket(fs::path full_path)
      : settings_{}, full_path_(std::move(full_path)), name_(full_path_.filename().string()), entry_map_() {
    if (!fs::exists(full_path_)) {
      throw std::runtime_error(fmt::format("Path '{}' doesn't exist", full_path_.string()));
    }

    const auto settings_path = full_path_ / kSettingsName;
    std::ifstream settings_file(settings_path, std::ios::binary);
    if (!settings_file) {
      throw std::runtime_error(fmt::format("Failed to open file {}", settings_path.string()));
    }

    settings_.ParseFromIstream(&settings_file);


    for (const auto& folder : fs::directory_iterator(full_path_)) {
      if (fs::is_directory(folder)) {
        auto entry_name = folder.path().filename().string();
        auto entry = IEntry::Build(IEntry::Options{
            .name = folder.path().filename().string(),
            .path = folder.path().parent_path().string(),
            .max_block_size = settings_.max_block_size(),
            .max_block_records = settings_.max_block_records(),
        });
        if (entry) {
          entry_map_[entry_name] = std::move(entry);
        } else {
          LOG_ERROR("Failed to restore entry '{}'", entry_name);
        }
      }
    }
  }

  EntryRef GetOrCreateEntry(const std::string& name) override {
    auto it = entry_map_.find(name);
    if (it != entry_map_.end()) {
      return {it->second, Error::kOk};
    } else {
      LOG_DEBUG("No '{}' entry in a bucket. Try to create one", name);
      auto entry = IEntry::Build({
          .name = name,
          .path = full_path_,
          .max_block_size = settings_.max_block_size(),
          .max_block_records = settings_.max_block_records(),
      });

      if (entry) {
        std::shared_ptr<IEntry> ptr = std::move(entry);
        entry_map_[name] = ptr;
        return {ptr, Error::kOk};
      }
    }

    return {std::weak_ptr<IEntry>(), Error{.code = 500, .message = fmt::format("Failed to create bucket '{}'", name)}};
  }

  [[nodiscard]] Error Clean() override {
    fs::remove_all(full_path_);
    entry_map_ = {};
    return Error::kOk;
  }

  [[nodiscard]] Error KeepQuota() override {
    auto err = Error::kOk;
    switch (settings_.quota_type()) {
      case BucketSettings::NONE:
        break;
      case BucketSettings::FIFO:
        size_t bucket_size = GetInfo().size();
        while (bucket_size > settings_.quota_size()) {
          LOG_DEBUG("Size of bucket '{}' is {} bigger than quota {}. Remove the oldest record",
                    full_path_.filename().string(), bucket_size, settings_.quota_size());

          auto rr =
              entry_map_ | std::views::values | std::views::transform([](auto entry) { return entry->GetInfo(); });

          std::vector<EntryInfo> candidates(std::ranges::begin(rr), std::ranges::end(rr));
          std::ranges::sort(candidates, [](auto& a, auto& b) { return a.oldest_record() < b.oldest_record(); });

          for (int i = 0; i < candidates.size(); ++i) {
            const auto& entry = candidates[i];
            LOG_DEBUG("Remove the oldest block in entry '{}'", entry.name());
            auto entry_ptr = entry_map_.at(entry.name());
            err = entry_ptr->RemoveOldestBlock();
            if (!err) {
              if (entry_ptr->GetInfo().block_count() == 0) {
                entry_map_.erase(entry.name());
                fs::remove(full_path_ / entry.name());
              }

              bucket_size = GetInfo().size();
              break;
            }
          }
        }
    }
    return err;
  }

  Error SetSettings(BucketSettings settings) override {
    settings_ = std::move(settings);
    return SaveDescriptor();
  }

  std::vector<EntryInfo> GetEntryList() const override {
    auto rr = entry_map_ | std::views::values | std::views::transform([](auto entry) { return entry->GetInfo(); });
    return std::vector(std::ranges::begin(rr), std::ranges::end(rr));
  }

  bool HasEntry(const std::string& name) const override { return entry_map_.contains(name); }

  [[nodiscard]] BucketInfo GetInfo() const override {
    size_t record_count = 0;
    size_t size = 0;
    uint64_t oldest_ts = std::numeric_limits<uint64_t>::max();
    uint64_t latest_ts = 0;

    for (const auto& [_, entry] : entry_map_) {
      auto info = entry->GetInfo();
      record_count += info.record_count();
      size += info.size();

      oldest_ts = std::min(oldest_ts, info.oldest_record());
      latest_ts = std::max(latest_ts, info.latest_record());
    }

    BucketInfo info;
    info.set_name(name_);
    info.set_size(size);
    info.set_entry_count(entry_map_.size());
    info.set_oldest_record(oldest_ts);
    info.set_latest_record(latest_ts);
    return info;
  }

  [[nodiscard]] const BucketSettings& GetSettings() const override { return settings_; }

 private:
  core::Error SaveDescriptor() const {
    const auto settings_path = full_path_ / kSettingsName;
    std::ofstream settings_file(settings_path, std::ios::binary);
    if (!settings_file) {
      return {
          .code = 500,
          .message = "Failed to open file of bucket settings",
      };
    }

    if (!settings_.SerializeToOstream(&settings_file)) {
      return {
          .code = 500,
          .message = "Failed to serialize bucket settings",
      };
    }

    return Error::kOk;
  }

  constexpr static std::string_view kSettingsName = "bucket.settings";

  fs::path full_path_;
  std::string name_;
  BucketSettings settings_;
  std::map<std::string, std::shared_ptr<IEntry>> entry_map_;
};

std::unique_ptr<IBucket> IBucket::Build(std::filesystem::path full_path, BucketSettings settings) {
  std::unique_ptr<IBucket> bucket;
  try {
    bucket = std::make_unique<Bucket>(std::move(full_path), std::move(settings));
  } catch (const std::runtime_error& err) {
    LOG_ERROR("Failed create bucket '{}': {}", full_path.string(), err.what());
  }

  return bucket;
}

std::unique_ptr<IBucket> IBucket::Restore(std::filesystem::path full_path) {
  try {
    return std::make_unique<Bucket>(std::move(full_path));
  } catch (const std::exception& err) {
    LOG_ERROR(err.what());
  }

  return nullptr;
}
const BucketSettings& IBucket::GetDefaults() {
  static BucketSettings default_settings;

  if (!default_settings.has_max_block_size()) {
    default_settings.set_max_block_size(kDefaultMaxBlockSize);
    default_settings.set_quota_type(BucketSettings::NONE);
    default_settings.set_quota_size(0);
    default_settings.set_max_block_records(kDefaultMaxBlockRecords);
  }

  return default_settings;
}

}  // namespace reduct::storage
