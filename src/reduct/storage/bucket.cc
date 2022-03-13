// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/bucket.h"

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

    if (!settings_.has_max_block_size()) {
      settings_.set_max_block_size(kDefaultMaxBlockSize);
    }

    if (!settings_.has_quota_type()) {
      settings_.set_quota_type(BucketSettings::NONE);
    }

    if (!settings_.has_quota_size()) {
      settings_.set_quota_size(0);
    }

    fs::create_directories(full_path_);
    auto err = SaveDescriptor();
    if (err) {
      throw std::runtime_error(err.message);
    }
  }

  explicit Bucket(fs::path full_path)
      : settings_{}, full_path_(std::move(full_path)), name_(full_path_.filename().string()),  entry_map_() {
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
        auto entry = IEntry::Restore(folder);
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
    switch (settings_.quota_type()) {
      case BucketSettings::NONE:
        break;
      case BucketSettings::FIFO:
        size_t bucket_size = GetInfo().size();
        while (bucket_size > settings_.quota_size()) {
          LOG_DEBUG("Size of bucket '{}' is {} bigger than quota {}. Remove the oldest record",
                    full_path_.filename().string(), bucket_size, settings_.quota_size());

          std::shared_ptr<IEntry> current_entry = nullptr;
          for (const auto& [_, entry] : entry_map_) {
            auto entry_info = entry->GetInfo();
            if (entry_info.block_count > 1 ||  // first not empty
                (current_entry && entry_info.oldest_record_time < current_entry->GetInfo().oldest_record_time)) {
              current_entry = entry;
            }
          }

          if (!current_entry) {
            LOG_DEBUG("Looks like all the entries have only one block");
            return Error::kOk;
          }

          LOG_DEBUG("Remove the oldest block in entry '{}'", current_entry->GetOptions().name);
          auto err = current_entry->RemoveOldestBlock();
          if (err) {
            return err;
          }

          bucket_size = GetInfo().size();
        }
    }
    return Error::kOk;
  }

  Error SetSettings(BucketSettings settings) override {
    settings_ = std::move(settings);
    return SaveDescriptor();
  }

  [[nodiscard]] BucketInfo GetInfo() const override {
    using Clk = IEntry::Time::clock;

    size_t record_count = 0;
    size_t size = 0;
    IEntry::Time oldest_ts = Clk::now();
    IEntry::Time latest_ts{};

    for (const auto& [_, entry] : entry_map_) {
      auto info = entry->GetInfo();
      record_count += info.record_count;
      size += info.bytes;

      oldest_ts = std::min(oldest_ts, info.oldest_record_time);
      latest_ts = std::max(latest_ts, info.latest_record_time);
    }

    BucketInfo info;
    info.set_name(name_);
    info.set_size(size);
    info.set_entry_count(entry_map_.size());
    info.set_oldest_record(Clk::to_time_t(oldest_ts));
    info.set_latest_record(Clk::to_time_t(latest_ts));
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

}  // namespace reduct::storage
