// Copyright 2021 Alexey Timin

#include "reduct/storage/bucket.h"

#include "reduct/core/logger.h"

namespace reduct::storage {

namespace fs = std::filesystem;

class Bucket : public IBucket {
 public:
  explicit Bucket(Options options) : options_(std::move(options)) {
    auto full_path = options_.path / options_.name;
    if (!fs::exists(full_path)) {
      LOG_DEBUG("Path {} doesn't exist. Create a new bucket.", full_path.string());
      fs::create_directories(full_path);
    }
  }

 private:
  Options options_;
};

std::unique_ptr<IBucket> IBucket::Build(Options options) {
  if (options.name.empty()) {
    LOG_ERROR("Bucket must have a name");
    return nullptr;
  }

  std::unique_ptr<IBucket> bucket;
  try {
    bucket = std::make_unique<Bucket>(options);
  } catch (const fs::filesystem_error &err) {
    LOG_ERROR("Failed create bucket '{}': {}", options.name, err.what());
  }

  return bucket;
}

}  // namespace reduct::storage