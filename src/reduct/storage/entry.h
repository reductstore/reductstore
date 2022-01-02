// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <ostream>

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

    bool operator==(const Options& rhs) const {
      return std::tie(name, path, min_block_size) == std::tie(rhs.name, rhs.path, rhs.min_block_size);
    }

    bool operator!=(const Options& rhs) const { return !(rhs == *this); }
  };

  using Time = std::chrono::system_clock::time_point;
  struct ReadResult {
    std::string data;
    core::Error error;
    Time time;

    bool operator==(const ReadResult& rhs) const {
      return std::tie(data, error, time) == std::tie(rhs.data, rhs.error, rhs.time);
    }

    bool operator!=(const ReadResult& rhs) const { return !(rhs == *this); }

    friend std::ostream& operator<<(std::ostream& os, const ReadResult& result);
  };

  struct Info {
    size_t block_count;
    size_t record_count;
    size_t bytes;

    bool operator==(const Info& rhs) const {
      return std::tie(block_count, record_count, bytes) == std::tie(rhs.block_count, rhs.record_count, rhs.bytes);
    }

    bool operator!=(const Info& rhs) const { return !(rhs == *this); }

    friend std::ostream& operator<<(std::ostream& os, const Info& info);
  };

  [[nodiscard]] virtual core::Error Write(std::string&& blob, const Time& time) = 0;
  [[nodiscard]] virtual ReadResult Read(const Time& time) const = 0;
  [[nodiscard]] virtual Info GetInfo() const = 0;
  [[nodiscard]] virtual const Options& GetOptions() const = 0;


  static std::unique_ptr<IEntry> Build(Options options);
  static std::unique_ptr<IEntry> Restore(std::filesystem::path full_path);

};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_ENTRY_H
