// Copyright 2021-2022 Alexey Timin
#include "reduct/storage/bucket.h"

#include <fstream>
#include <ranges>

#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/proto/bucket.pb.h"

namespace reduct::storage {

namespace fs = std::filesystem;
using core::Error;
using proto::BucketSettings;

class Bucket : public IBucket {
 public:
  explicit Bucket(Options options) : options_(std::move(options)), entry_map_() {
    full_path_ = options_.path / options_.name;
    if (fs::exists(full_path_)) {
      throw std::runtime_error(fmt::format("Path '{}' already exists", full_path_.string()));
    }

    fs::create_directories(full_path_);

    BucketSettings settings;
    settings.mutable_quota()->set_type(static_cast<proto::BucketSettings_QuotaType>(options.quota.type));
    settings.mutable_quota()->set_size(options.quota.size);

    const auto settings_path = full_path_ / kSettingsName;
    std::ofstream settings_file(settings_path, std::ios::binary);
    if (!settings_file) {
      throw std::runtime_error(fmt::format("Failed to open file {}", settings_path.string()));
    }

    settings.SerializeToOstream(&settings_file);
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
          .max_block_size = kDefaultMaxBlockSize,
      });

      if (entry) {
        std::shared_ptr<IEntry> ptr = std::move(entry);
        entry_map_[name] = ptr;
        return {ptr, Error::kOk};
      }
    }

    return {std::weak_ptr<IEntry>(), Error{.code = 500, .message = fmt::format("Failed to create bucket '{}'", name)}};
  }

  Error Clean() override {
    fs::remove_all(full_path_);
    entry_map_ = {};
    return Error::kOk;
  }

  [[nodiscard]] Info GetInfo() const override {
    return {
        .entry_count = entry_map_.size(),
    };
  }

  [[nodiscard]] const Options& GetOptions() const override { return options_; }

 private:
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
  return std::make_unique<Bucket>(std::move(full_path));
}

std::ostream& operator<<(std::ostream& os, const IBucket::Options& options) {
  std::stringstream quota_ss;
  quota_ss << options.quota;
  os << fmt::format("<IBucket::Options name={}, path={}, quota={}>", options.name, options.path.string(),
                    quota_ss.str());
  return os;
}
std::ostream& operator<<(std::ostream& os, const IBucket::QuotaOptions& options) {
  os << fmt::format("<IBucket::QuotaOptions type={}, size={}>", static_cast<int>(options.type), options.size);
  return os;
}
}  // namespace reduct::storage
