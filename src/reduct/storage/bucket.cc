// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/bucket.h"

#include <fstream>
#include <numeric>
#include <ranges>

#include "reduct/core/logger.h"
#include "reduct/proto/bucket.pb.h"

namespace reduct::storage {

namespace fs = std::filesystem;
using core::Error;
using proto::BucketSettings;

std::pair<IBucket::QuotaType, Error> IBucket::ParseQuotaType(std::string_view quota_type_str) {
  IBucket::QuotaType type;

  if (quota_type_str == "NONE") {
    type = IBucket::QuotaType ::kNone;
  } else if (quota_type_str == "FIFO") {
    type = IBucket::QuotaType ::kFifo;
  } else {
    return {type, Error{.code = 422, .message = fmt::format("Unknown type of quota: {}", quota_type_str)}};
  }

  return {type, Error::kOk};
}

std::string_view IBucket::GetQuotaTypeName(QuotaType type) {
  switch (type) {
    case kNone:
      return "NONE";
    case kFifo:
      return "FIFO";
  }

  return "Unknown type";
}

class Bucket : public IBucket {
 public:
  explicit Bucket(Options options) : options_(std::move(options)), entry_map_() {
    full_path_ = options_.path / options_.name;
    if (fs::exists(full_path_)) {
      throw std::runtime_error(fmt::format("Path '{}' already exists", full_path_.string()));
    }

    fs::create_directories(full_path_);
    auto err = SaveDescriptor();
    if (err) {
      throw std::runtime_error(err.message);
    }
  }

  explicit Bucket(fs::path full_path) : options_{}, full_path_(std::move(full_path)), entry_map_() {
    if (!fs::exists(full_path_)) {
      throw std::runtime_error(fmt::format("Path '{}' doesn't exist", full_path_.string()));
    }

    const auto settings_path = full_path_ / kSettingsName;
    std::ifstream settings_file(settings_path, std::ios::binary);
    if (!settings_file) {
      throw std::runtime_error(fmt::format("Failed to open file {}", settings_path.string()));
    }

    BucketSettings settings;
    settings.ParseFromIstream(&settings_file);

    options_ = Options{
        .name = full_path_.filename(),
        .path = full_path_.parent_path(),
        .max_block_size = settings.max_block_size(),
        .quota = {.type = static_cast<QuotaType>(settings.quota().type()), .size = settings.quota().size()},
    };

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
          .max_block_size = options_.max_block_size,
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
    switch (options_.quota.type) {
      case IBucket::kNone:
        break;
      case IBucket::kFifo:
        size_t bucket_size = GetInfo().size;
        while (bucket_size > options_.quota.size) {
          LOG_DEBUG("Size of bucket '{}' is {} bigger than quota {}. Remove the oldest record", options_.name,
                    bucket_size, options_.quota.size);

          std::shared_ptr<IEntry> current_entry = nullptr;
          for (const auto& [_, entry] : entry_map_) {
            auto entry_info = entry->GetInfo();
            if (entry_info.block_count > 1  ||  // first not empty
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

          bucket_size = GetInfo().size;
        }
    }
    return Error::kOk;
  }

  Error SetOptions(Options options) override {
    options_.max_block_size = options.max_block_size;
    options_.quota = options.quota;

    return SaveDescriptor();
  }

  [[nodiscard]] Info GetInfo() const override {
    size_t record_count = 0;
    size_t size = 0;
    for (const auto& [_, entry] : entry_map_) {
      auto info = entry->GetInfo();
      record_count += info.record_count;
      size += info.bytes;
    }

    return {.entry_count = entry_map_.size(), .record_count = record_count, .size = size};
  }

  [[nodiscard]] const Options& GetOptions() const override { return options_; }

 private:
  core::Error SaveDescriptor() const {
    BucketSettings settings;
    settings.set_max_block_size(options_.max_block_size);
    settings.mutable_quota()->set_type(static_cast<proto::BucketSettings_QuotaType>(options_.quota.type));
    settings.mutable_quota()->set_size(options_.quota.size);

    const auto settings_path = full_path_ / kSettingsName;
    std::ofstream settings_file(settings_path, std::ios::binary);
    if (!settings_file) {
      return {
          .code = 500,
          .message = "Failed to open file of bucket settings",
      };
    }

    if (!settings.SerializeToOstream(&settings_file)) {
      return {
          .code = 500,
          .message = "Failed to serialize bucket settings",
      };
    }

    return Error::kOk;
  }

  constexpr static std::string_view kSettingsName = "bucket.settings";
  Options options_;
  fs::path full_path_;
  std::map<std::string, std::shared_ptr<IEntry>> entry_map_;
};

std::unique_ptr<IBucket> IBucket::Build(const Options& options) {
  if (options.name.empty()) {
    LOG_ERROR("Bucket must have a name");
    return nullptr;
  }

  std::unique_ptr<IBucket> bucket;
  try {
    bucket = std::make_unique<Bucket>(options);
  } catch (const std::runtime_error& err) {
    LOG_ERROR("Failed create bucket '{}': {}", options.name, err.what());
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

std::ostream& operator<<(std::ostream& os, const IBucket::Options& options) {
  std::stringstream quota_ss;
  quota_ss << options.quota;
  os << fmt::format("<IBucket::Options name={}, path={}, max_block_size={}, quota={}>", options.name,
                    options.path.string(), options.max_block_size, quota_ss.str());
  return os;
}
std::ostream& operator<<(std::ostream& os, const IBucket::QuotaOptions& options) {
  os << fmt::format("<IBucket::QuotaOptions type={}, bucket_size={}>", static_cast<int>(options.type), options.size);
  return os;
}
std::ostream& operator<<(std::ostream& os, const IBucket::Info& info) {
  os << fmt::format("<IBucket::Info entry_count={} record_count={} quota_size={}>", info.entry_count, info.record_count,
                    info.size);
  return os;
}
}  // namespace reduct::storage
