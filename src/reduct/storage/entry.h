// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>

#include "reduct/core/error.h"

namespace reduct::storage {

/**
 *
 */
class IEntry {
 public:
  struct Options {
    std::string name;
    std::filesystem::path path;
    size_t min_block_size;
  };

  using Time = std::chrono::system_clock::time_point;
  struct ReadResult {
    std::string data;
    core::Error error;
    Time time;
  };

  [[nodiscard]] virtual core::Error Write(std::string&& blob, const Time& time) = 0;
  [[nodiscard]] virtual ReadResult Read(const Time& time) const = 0;

  static std::unique_ptr<IEntry> Build(Options options);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_ENTRY_H
