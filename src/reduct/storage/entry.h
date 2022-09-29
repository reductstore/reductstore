// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_H
#define REDUCT_STORAGE_ENTRY_H

#include <filesystem>
#include <ostream>
#include <vector>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/core/time.h"
#include "reduct/proto/api/entry.pb.h"
#include "reduct/storage/io/async_io.h"
#include "reduct/storage/query/quiery.h"

namespace reduct::storage {

/**
 *  Entry of a bucket. Store history of blobs as time series
 */
class IEntry : public io::IAsyncIO, public query::IQuery {
 public:
  using UPtr = std::unique_ptr<IEntry>;
  using SPtr = std::shared_ptr<IEntry>;
  using WPtr = std::weak_ptr<IEntry>;

  virtual ~IEntry() = default;

  /**
   * Options
   */
  struct Options {
    size_t max_block_size;     // max block quota_size after that we create a new one
    size_t max_block_records;  // max number of records in a block

    std::strong_ordering operator<=>(const Options& rhs) const = default;
  };

  /**
   * @brief Remove the oldest block from disk
   * @return
   */
  virtual core::Error RemoveOldestBlock() = 0;
  /**
   * @brief Provides statistical information about the entry
   * @return
   */
  [[nodiscard]] virtual proto::api::EntryInfo GetInfo() const = 0;

  /**
   * @brief Provides current options of the entry
   * @return
   */
  [[nodiscard]] virtual const Options& GetOptions() const = 0;

  /**
   * @brief Set options
   * @return
   */
  virtual void SetOptions(const Options&) = 0;

  /**
   * @brief Creates a new entry.
   * Directory path/name must be empty
   * @param options
   * @return pointer to entre or nullptr if failed to create
   */
  static IEntry::UPtr Build(std::string_view name, const std::filesystem::path& path, Options options);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_ENTRY_H
