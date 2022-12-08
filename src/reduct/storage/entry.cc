// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <fmt/core.h>
#include <google/protobuf/util/time_util.h>

#include <filesystem>
#include <ranges>

#include "reduct/async/io.h"
#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/core/result.h"
#include "reduct/proto/storage/entry.pb.h"
#include "reduct/storage/block_manager.h"
#include "reduct/storage/io/async_reader.h"
#include "reduct/storage/io/async_writer.h"

namespace reduct::storage {

using core::Error;
using core::Result;
using core::Time;
using io::AsyncReaderParameters;
using proto::api::EntryInfo;
using query::IQuery;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
auto to_time_t = core::Time::clock::to_time_t;

namespace fs = std::filesystem;

class Entry : public IEntry {
 public:
  /**
   * Create a new entry
   * @param options
   */
  Entry(std::string_view name, std::filesystem::path path, Options options)
      : name_(name), options_(std::move(options)), block_set_(), size_counter_{}, record_counter_{} {
    full_path_ = path / name_;
    block_manager_ = IBlockManager::Build(full_path_);
    if (!fs::create_directories(full_path_)) {
      for (const auto& file : fs::directory_iterator(full_path_)) {
        auto path = file.path();
        if (fs::is_regular_file(file) && path.extension() == kMetaExt) {
          try {
            auto ts = TimeUtil::MicrosecondsToTimestamp(std::stoull(path.stem().c_str()));
            auto [block, err] = block_manager_->LoadBlock(ts);

            if (err || block->begin_time().seconds() == 0 || block->invalid()) {
              LOG_WARNING("Block {} looks broken. Remove it.", path.string());
              std::error_code ec;
              if (!fs::remove(path, ec)) {
                LOG_ERROR("Failed to remove {}: {}", path.string(), ec.message());
              }

              path = path.parent_path() / fmt::format("{}{}", path.stem().string(), kBlockExt);
              if (!fs::remove(path, ec)) {
                LOG_ERROR("Failed to remove {}: {}", path.string(), ec.message());
              }
              continue;
            }

            block_set_.insert(ts);
            size_counter_ += block->size();
            record_counter_ += block->records_size();
          } catch (std::exception& err) {
            LOG_ERROR("Wrong filename format {}: {}", path.string(), err.what());
          }
        }
      }
    }
  }

  [[nodiscard]] Result<async::IAsyncWriter::SPtr> BeginWrite(const Time& time, size_t content_size) override {
    enum class RecordType { kLatest, kBelated, kBelatedFirst };
    RecordType type = RecordType::kLatest;

    const auto proto_ts = FromTimePoint(time);

    auto start_new_block = [this](const Timestamp& ts, size_t content_size) -> Result<IBlockManager::BlockSPtr> {
      auto [block, err] = block_manager_->StartBlock(ts, std::max(options_.max_block_size, content_size));
      if (err) {
        return {{}, err};
      }

      block_set_.insert(block->begin_time());
      return {block, Error::kOk};
    };

    auto get_block = [this, content_size, &start_new_block](auto ts) {
      if (!block_set_.empty()) {
        // Load last block if it exists
        return block_manager_->LoadBlock(*block_set_.rbegin());
      } else {
        return start_new_block(ts, content_size);
      }
    };

    auto [block, get_err] = get_block(proto_ts);
    if (get_err) {
      return {{}, std::move(get_err)};
    }

    if (block->has_latest_record_time() && block->latest_record_time() >= proto_ts) {
      LOG_DEBUG("Timestamp {} is belated. Finding proper block", TimeUtil::ToString(proto_ts));

      Result<IBlockManager::BlockSPtr> ret;
      if (*block_set_.begin() > proto_ts) {
        LOG_DEBUG("Timestamp earlier than first record");
        type = RecordType::kBelatedFirst;
        ret = start_new_block(proto_ts, content_size);
      } else {
        type = RecordType::kBelated;
        ret = FindBlock(proto_ts);
      }

      if (ret.error) {
        return {{}, ret.error};
      }
      block = ret.result;
      // Check if block doesn't have the record already
      auto exist =
          std::ranges::any_of(block->records(), [proto_ts](auto& record) { return record.timestamp() == proto_ts; });
      if (exist) {
        return {{},
                {.code = 409,
                 .message = fmt::format("A record with timestamp {} already exists",
                                        TimeUtil::TimestampToMicroseconds(proto_ts))}};
      }
    }

    if (!block->has_begin_time()) {
      LOG_DEBUG("First record_entry for current block");
      block->mutable_begin_time()->CopyFrom(proto_ts);
    }

    auto has_no_space = block->size() + content_size > options_.max_block_size;
    auto too_many_records = block->records_size() + 1 > options_.max_block_records;

    if (type == RecordType::kLatest && (has_no_space || too_many_records || block->invalid())) {
      LOG_DEBUG("Create a new block");
      if (auto err = block_manager_->FinishBlock(block)) {
        LOG_WARNING("Failed to finish the current block: {}", err.ToString());
      }

      auto ret = start_new_block(proto_ts, content_size);
      if (ret.error) {
        LOG_ERROR("Failed to create a next block");
        return {{}, ret.error};
      }

      block = std::move(ret.result);
    }

    // Update writing block
    auto record = block->add_records();
    record->set_state(proto::Record::kStarted);
    record->mutable_timestamp()->CopyFrom(proto_ts);
    record->set_begin(block->size());
    record->set_end(block->size() + content_size);

    block->set_size(block->size() + content_size);

    // Update counters
    record_counter_++;
    size_counter_ += content_size;

    switch (type) {
      case RecordType::kLatest:
        block->mutable_latest_record_time()->CopyFrom(proto_ts);
        break;
      case RecordType::kBelatedFirst:
        block->mutable_begin_time()->CopyFrom(proto_ts);
        break;
      case RecordType::kBelated:
        break;
    }

    if (auto err = block_manager_->SaveBlock(block)) {
      return {{}, std::move(err)};
    }

    return block_manager_->BeginWrite(block, {
                                                 .path = BlockPath(full_path_, *block),
                                                 .record_index = block->records_size() - 1,
                                                 .size = content_size,
                                             });
  }

  [[nodiscard]] Result<async::IAsyncReader::SPtr> BeginRead(const Time& time) const override {
    const auto proto_ts = FromTimePoint(time);

    LOG_DEBUG("Read a record for ts={}", TimeUtil::ToString(proto_ts));

    if (block_set_.empty() || proto_ts < *block_set_.begin()) {
      return {{}, {.code = 404, .message = "No records for this timestamp"}};
    }

    if (auto err = CheckLatestRecord(proto_ts)) {
      return {{}, std::move(err)};
    }

    auto [block, err] = FindBlock(proto_ts);
    if (err) {
      LOG_ERROR("No block in entry '{}' for ts={}", name_, TimeUtil::ToString(proto_ts));
      return {{}, {.code = 500, .message = "Failed to find the needed block in descriptor"}};
    }

    int record_index = -1;
    for (int i = 0; i < block->records_size(); ++i) {
      const auto& current_record = block->records(i);
      if (current_record.timestamp() == proto_ts) {
        record_index = i;
        break;
      }
    }

    if (record_index == -1) {
      return {{}, {.code = 404, .message = "No records for this timestamp"}};
    }

    auto block_path = BlockPath(full_path_, *block);
    LOG_DEBUG("Found block {} with needed record", block_path.string());

    auto record = block->records(record_index);
    if (record.state() == proto::Record::kStarted) {
      return {{}, {.code = 425, .message = "Record is still being written"}};
    }

    if (record.state() == proto::Record::kErrored) {
      return {{}, {.code = 500, .message = "Record is broken"}};
    }

    return block_manager_->BeginRead(
        block, AsyncReaderParameters{
                   .path = block_path, .record_index = record_index, .chunk_size = kDefaultMaxReadChunk, .time = time});
  }

  core::Result<uint64_t> Query(const std::optional<Time>& start, const std::optional<Time>& stop,
                               const query::IQuery::Options& options) override {
    static uint64_t query_id = 0;

    RemoveOutDatedQueries();

    const auto current_time = Time::clock::now();
    queries_[query_id] = QueryInfo{
        .start = (start ? *start : Time::min()),
        .stop = (stop ? *stop : Time::max()),
        .last_update = Time::clock::now(),
        .options = options,
    };

    return {query_id++, Error::kOk};
  }

  Result<NextRecord> Next(uint64_t query_id) const override {
    RemoveOutDatedQueries();

    if (!queries_.contains(query_id)) {
      return {{},
              {.code = 404, .message = fmt::format("Query id={} doesn't exist. It expired or was finished", query_id)}};
    }

    if (block_set_.empty()) {
      return {{}, {.code = 202, .message = "No Content"}};
    }

    auto& query_info = queries_[query_id];
    query_info.last_update = Time::clock::now();

    auto start_ts = FromTimePoint(query_info.start);
    if (query_info.next_record) {
      start_ts = FromTimePoint(*query_info.next_record);
    }
    auto stop_ts = FromTimePoint(query_info.stop);

    // Find start block
    auto start_block = block_set_.upper_bound(start_ts);
    if (start_block == block_set_.end()) {
      start_block = std::prev(start_block);
    } else if (start_block != block_set_.begin()) {
      start_block = std::prev(start_block);
    }

    auto [block, err] = block_manager_->LoadBlock(*start_block);
    if (err) {
      queries_.erase(query_id);
      return {{}, err};
    }

    // Check if it is valid
    if (block->invalid()) {
      start_block = std::next(start_block);
      if (start_block != std::end(block_set_) && *start_block < stop_ts) {
        const auto [next_block, next_err] = block_manager_->LoadBlock(*start_block);
        if (next_err) {
          queries_.erase(query_id);
          return {{}, next_err};
        }

        block = next_block;
      } else {
        queries_.erase(query_id);
        return {{}, {.code = 202, .message = "No Content"}};
      }
    }

    std::vector<int> records;
    records.reserve(block->records_size());
    for (auto record_index = 0; record_index < block->records_size(); ++record_index) {
      const auto& record = block->records(record_index);
      if (record.timestamp() >= start_ts && record.timestamp() < stop_ts &&
          record.state() == proto::Record::kFinished) {
        records.push_back(record_index);
      }
    }

    if (records.empty()) {
      queries_.erase(query_id);
      return {{}, {.code = 202, .message = "No Content"}};
    }

    auto get_timestamp = [block](int index) { return block->records(index).timestamp(); };
    std::ranges::sort(records, {}, get_timestamp);
    auto& record_index = records[0];

    bool last = false;
    if (records.size() > 1) {
      query_info.next_record = ToTimePoint(get_timestamp(records[1]));
    } else {
      // Only one record in current block check next one
      auto next_block_it = std::next(start_block);
      if (next_block_it != std::end(block_set_)) {
        if (*next_block_it < stop_ts) {
          query_info.next_record = ToTimePoint(*next_block_it);
        } else {
          // no records in next block
          last = true;
        }
      } else {
        // no next block
        last = true;
      }
    }

    if (last) {
      queries_.erase(query_id);
    }

    auto [reader, reader_err] =
        block_manager_->BeginRead(block, AsyncReaderParameters{.path = BlockPath(full_path_, *block),
                                                               .record_index = record_index,
                                                               .chunk_size = kDefaultMaxReadChunk,
                                                               .time = ToTimePoint(get_timestamp(record_index))});
    if (reader_err) {
      return {{}, reader_err};
    }

    NextRecord next_record{
        .reader = reader,
        .last = last,
    };

    return {next_record, Error::kOk};
  }

  Error RemoveOldestBlock() override {
    if (block_set_.empty()) {
      return {.code = 500, .message = "Tries to remove a block in empty entry"};
    }

    auto [first_block, err] = block_manager_->LoadBlock(*block_set_.begin());
    if (err) {
      return err;
    }

    if (auto remove_err = block_manager_->RemoveBlock(first_block)) {
      return remove_err;
    }

    size_counter_ -= first_block->size();
    record_counter_ -= first_block->records_size();
    block_set_.erase(block_set_.begin());
    return Error::kOk;
  }

  [[nodiscard]] EntryInfo GetInfo() const override {
    Timestamp oldest_record, latest_record;
    if (!block_set_.empty()) {
      auto [latest_block, err] = block_manager_->LoadBlock(*block_set_.rbegin());
      if (err) {
        LOG_ERROR("{}", err.ToString());
      }

      oldest_record = *block_set_.begin();
      latest_record = latest_block->latest_record_time();
    }

    EntryInfo info;
    info.set_name(name_);
    info.set_size(size_counter_);
    info.set_record_count(record_counter_);
    info.set_block_count(block_set_.size());
    info.set_oldest_record(TimeUtil::TimestampToMicroseconds(oldest_record));
    info.set_latest_record(TimeUtil::TimestampToMicroseconds(latest_record));

    return info;
  }

  [[nodiscard]] const Options& GetOptions() const override { return options_; }

  void SetOptions(const Options& options) override { options_ = options; }

 private:
  Result<IBlockManager::BlockSPtr> FindBlock(Timestamp proto_ts) const {
    auto ts = block_set_.upper_bound(proto_ts);
    if (ts == block_set_.end()) {
      proto_ts = *block_set_.rbegin();
    } else {
      proto_ts = *std::prev(ts);
    }

    return block_manager_->LoadBlock(proto_ts);
  }

  static google::protobuf::Timestamp FromTimePoint(const Time& time) {
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time.time_since_epoch()).count();
    return TimeUtil::MicrosecondsToTimestamp(microseconds);
  }

  static Time ToTimePoint(const google::protobuf::Timestamp& time) {
    return Time() + std::chrono::microseconds(TimeUtil::TimestampToMicroseconds(time));
  }

  Error CheckLatestRecord(const Timestamp& proto_ts) const {
    auto [block, err] = block_manager_->LoadBlock(*block_set_.rbegin());
    if (err) {
      return err;
    }

    if (block->latest_record_time() < proto_ts) {
      return {.code = 404, .message = "No records for this timestamp"};
    }

    return Error::kOk;
  }

  void RemoveOutDatedQueries() const {
    const auto current_time = Time::clock::now();

    std::erase_if(queries_, [current_time](const auto& item) {
      auto const& [id, query] = item;
      return query.last_update + query.options.ttl < current_time;
    });
  }

  std::string name_;
  Options options_;
  fs::path full_path_;

  std::set<google::protobuf::Timestamp> block_set_;
  std::shared_ptr<IBlockManager> block_manager_;
  size_t size_counter_;
  size_t record_counter_;

  struct QueryInfo {
    Time start;
    Time stop;
    std::optional<Time> next_record;
    Time last_update;

    query::IQuery::Options options;
  };

  mutable std::unordered_map<uint64_t, QueryInfo> queries_;
};

IEntry::UPtr IEntry::Build(std::string_view name, const fs::path& path, IEntry::Options options) {
  return std::make_unique<Entry>(name, path, options);
}

};  // namespace reduct::storage
