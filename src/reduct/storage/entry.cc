// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <fmt/core.h>
#include <google/protobuf/util/time_util.h>

#include <filesystem>

#include "reduct/async/io.h"
#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/core/result.h"
#include "reduct/proto/storage/entry.pb.h"
#include "reduct/storage/async_reader.h"
#include "reduct/storage/async_writer.h"
#include "reduct/storage/block_manager.h"

namespace reduct::storage {

using core::Error;
using core::Result;
using proto::api::EntryInfo;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
auto to_time_t = IEntry::Time::clock::to_time_t;

namespace fs = std::filesystem;

class Entry : public IEntry {
 public:
  /**
   * Create a new entry
   * @param options
   */
  explicit Entry(Options options) : options_(std::move(options)), block_set_(), size_counter_{}, record_counter_{} {
    full_path_ = options_.path / options_.name;
    block_manager_ = IBlockManager::Build(full_path_);
    if (!fs::create_directories(full_path_)) {
      for (const auto& file : fs::directory_iterator(full_path_)) {
        auto& path = file.path();
        if (fs::is_regular_file(file) && path.extension() == kMetaExt) {
          try {
            auto ts = TimeUtil::MicrosecondsToTimestamp(std::stoul(path.stem().c_str()));
            auto [block, err] = block_manager_->LoadBlock(ts);
            if (err) {
              LOG_ERROR("{}", err.ToString());
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

  [[nodiscard]] Result<async::IAsyncWriter::UPtr> BeginWrite(const Time& time, size_t content_size) override {
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
    }

    if (!block->has_begin_time()) {
      LOG_DEBUG("First record_entry for current block");
      block->mutable_begin_time()->CopyFrom(proto_ts);
    }

    auto has_no_space = block->size() + content_size > options_.max_block_size;
    auto too_many_records = block->records_size() + 1 > options_.max_block_records;

    if (type == RecordType::kLatest && (has_no_space || too_many_records)) {
      LOG_DEBUG("Create a new block");
      if (auto err = block_manager_->FinishBlock(block)) {
        LOG_ERROR("Failed to finish the current block");
        return {{}, err};
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

    auto writer = BuildAsyncWriter(
        *block,
        {.path = BlockPath(full_path_, *block), .record_index = block->records_size() - 1, .size = content_size},
        [block_manager = block_manager_, ts = block->begin_time()](int index, auto state) {
          auto [block, load_err] = block_manager->LoadBlock(ts);
          if (load_err) {
            LOG_ERROR("{}", load_err.ToString());
          }
          block->mutable_records(index)->set_state(state);
          if (auto err = block_manager->SaveBlock(block)) {
            LOG_ERROR("{}", err.ToString());
          }
        });

    return {std::move(writer), Error::kOk};
  }

  [[nodiscard]] Result<async::IAsyncReader::UPtr> BeginRead(const Time& time) const override {
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
      LOG_ERROR("No block in entry '{}' for ts={}", options_.name, TimeUtil::ToString(proto_ts));
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

    return {BuildAsyncReader(*block,
                             AsyncReaderParameters{
                                 .path = block_path, .record_index = record_index, .chunk_size = kDefaultMaxReadChunk}),
            Error::kOk};
  }

  [[nodiscard]] core::Result<std::vector<RecordInfo>> List(const Time& start, const Time& stop) const override {
    auto start_ts = FromTimePoint(start);
    auto stop_ts = FromTimePoint(stop);
    LOG_DEBUG("List records for interval: ({}, {})", TimeUtil::ToString(start_ts), TimeUtil::ToString(stop_ts));
    if (start_ts > stop_ts) {
      return {{}, {.code = 422, .message = "Start timestamp cannot be older stop timestamp"}};
    }

    // Check boarders (is it okay if at least one record inside the interval
    if (block_set_.empty()) {
      return {{}, {.code = 404, .message = "No records in the entry"}};
    }

    if (stop_ts < *block_set_.begin()) {
      return {{}, {.code = 404, .message = "No records for time interval"}};
    }

    if (auto err = CheckLatestRecord(start_ts)) {
      return {{}, {.code = 404, .message = "No records for time interval"}};
    }

    // Find block range
    auto start_block = block_set_.upper_bound(start_ts);
    if (start_block == block_set_.end()) {
      start_block = block_set_.begin();
    } else if (start_block != block_set_.begin()) {
      start_block = std::prev(start_block);
    }

    auto stop_block = block_set_.lower_bound(stop_ts);

    std::vector<RecordInfo> records;
    for (auto block_it = start_block; block_it != stop_block; ++block_it) {
      auto [block, err] = block_manager_->LoadBlock(*block_it);
      if (err) {
        return {{}, err};
      }

      for (auto record_index = 0; record_index < block->records_size(); ++record_index) {
        const auto& record = block->records(record_index);
        if (record.timestamp() >= start_ts && record.timestamp() < stop_ts &&
            record.state() == proto::Record::kFinished) {
          records.push_back(RecordInfo{.time = ToTimePoint(record.timestamp()), .size = record.end() - record.begin()});
        }
      }
    }

    if (records.empty()) {
      return {{}, {.code = 404, .message = "No records for time interval"}};
    }

    std::ranges::sort(records, {}, &RecordInfo::time);
    return {records, {}};
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
    block_set_.erase(first_block->begin_time());
    return {};
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
    info.set_name(options_.name);
    info.set_size(size_counter_);
    info.set_record_count(record_counter_);
    info.set_block_count(block_set_.size());
    info.set_oldest_record(TimeUtil::TimestampToMicroseconds(oldest_record));
    info.set_latest_record(TimeUtil::TimestampToMicroseconds(latest_record));

    return info;
  }

  [[nodiscard]] const Options& GetOptions() const override { return options_; }

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

  Error CheckLatestRecord(const google::protobuf::Timestamp& proto_ts) const {
    auto [block, err] = block_manager_->LoadBlock(*block_set_.rbegin());
    if (err) {
      return err;
    }

    if (block->latest_record_time() < proto_ts) {
      return {.code = 404, .message = "No records for this timestamp"};
    }

    return Error::kOk;
  }

  Options options_;
  fs::path full_path_;

  std::set<google::protobuf::Timestamp> block_set_;
  std::shared_ptr<IBlockManager> block_manager_;
  size_t size_counter_;
  size_t record_counter_;
};

std::unique_ptr<IEntry> IEntry::Build(IEntry::Options options) { return std::make_unique<Entry>(std::move(options)); }

/**
 * Streams
 */

std::ostream& operator<<(std::ostream& os, const IEntry::RecordInfo& info) {
  os << fmt::format("<IEntry::RecordInfo time={}, size={}>", info.time.time_since_epoch().count() / 1000, info.size);
  return os;
}
};  // namespace reduct::storage
