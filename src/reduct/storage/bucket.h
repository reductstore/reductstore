// Copyright 2021 Alexey Timin
#ifndef REDUCT_STORAGE_BUCKET_H
#define REDUCT_STORAGE_BUCKET_H

#include <filesystem>

namespace reduct::storage {

class IBucket {
 public:
  struct Options {
    std::string  name;
    std::filesystem::path path;
  };

  static std::unique_ptr<IBucket> Build(const Options& options);
};

}  // namespace reduct::storage
#endif  // REDUCT_STORAGE_BUCKET_H
