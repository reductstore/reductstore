// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/storage/query/query.h"

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>

#include <vector>

#include "reduct/config.h"
#include "reduct/storage/block_manager.h"
#include "reduct/storage/io/async_reader.h"

namespace reduct::storage::query {

using core::Error;
using core::Result;
using core::Time;

using io::AsyncReaderParameters;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

struct QueryInfo {
  Time start;
  Time stop;
  std::optional<Time> next_record;
  Time last_update;

  query::IQuery::Options options;
};

static google::protobuf::Timestamp FromTimePoint(const Time& time) {
  auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time.time_since_epoch()).count();
  return TimeUtil::MicrosecondsToTimestamp(microseconds);
}

static Time ToTimePoint(const google::protobuf::Timestamp& time) {
  return Time() + std::chrono::microseconds(TimeUtil::TimestampToMicroseconds(time));
}

std::vector<int> QueryRecordsInBlock(const IBlockManager::BlockSPtr& block, const QueryInfo& query_info,
                                     const Timestamp& start_ts, const Timestamp& stop_ts) {
  std::vector<int> records;
  records.reserve(block->records_size());
  for (auto record_index = 0; record_index < block->records_size(); ++record_index) {
    const auto& record = block->records(record_index);
    bool in_time_interval =
        record.timestamp() >= start_ts && record.timestamp() < stop_ts && record.state() == proto::Record::kFinished;
    if (!in_time_interval) continue;

    // check if a record has value of labels that match include
    if (!query_info.options.include.empty()) {
      auto include = query_info.options.include;
      for (const auto& label : record.labels()) {
        if (include.contains(label.name()) && label.value() == include[label.name()]) {
          include.erase(label.name());
        }
      }

      if (!include.empty()) continue;
    }

    // check if a record has value of labels that does not match exclude
    if (!query_info.options.exclude.empty()) {
      auto exclude = query_info.options.exclude;
      for (const auto& label : record.labels()) {
        if (exclude.contains(label.name()) && label.value() == exclude[label.name()]) {
          exclude.erase(label.name());
        }
      }

      if (exclude.empty()) continue;
    }

    records.push_back(record_index);
  }

  return records;
}

class HistoricalQuery : public IQuery {
 public:
  HistoricalQuery(const Time& start, const Time& stop, const Options& options) {
    info_ = QueryInfo{
        .start = start,
        .stop = stop,
        .next_record = std::nullopt,
        .last_update = Time::clock::now(),
        .options = options,
    };
  }

  Result<NextRecord> Next(const std::set<google::protobuf::Timestamp>& blocks, IBlockManager* block_manager) override {
    info_.last_update = Time::clock::now();

    auto start_ts = FromTimePoint(info_.start);
    if (info_.next_record) {
      start_ts = FromTimePoint(*info_.next_record);
    }
    auto stop_ts = FromTimePoint(info_.stop);

    // Find start block
    auto start_block_it = blocks.upper_bound(start_ts);
    if (start_block_it == blocks.end()) {
      start_block_it = std::prev(start_block_it);
    } else if (start_block_it != blocks.begin()) {
      start_block_it = std::prev(start_block_it);
    }

    std::vector<int> records;
    IBlockManager::BlockSPtr block;
    Error search_err = Error::kOk;
    for (auto it = start_block_it; it != std::end(blocks) && *it < stop_ts; it++) {
      auto ret = block_manager->LoadBlock(*it);
      if (ret.error) {
        search_err = std::move(ret.error);
        break;
      }

      block = ret.result;
      if (block->invalid()) {
        continue;
      }
      // Find records
      records = QueryRecordsInBlock(block, info_, start_ts, stop_ts);
      if (!records.empty() || info_.options.include.empty()) {
        break;
      }
    }

    RETURN_ERROR(search_err);

    if (records.empty()) {
      return Error::NoContent();
    }

    auto get_timestamp = [block](int index) { return block->records(index).timestamp(); };
    std::ranges::sort(records, {}, get_timestamp);
    auto& record_index = records[0];

    bool last = false;
    if (records.size() > 1) {
      info_.next_record = ToTimePoint(get_timestamp(records[1]));
    } else {
      // Only one record in current block check next one
      auto next_block_it = std::next(blocks.find(block->begin_time()));
      if (next_block_it != std::end(blocks)) {
        if (*next_block_it < stop_ts) {
          info_.next_record = ToTimePoint(*next_block_it);
        } else {
          // no records in next block
          last = true;
        }
      } else {
        // no next block
        last = true;
      }
    }

    auto [reader, reader_err] =
        block_manager->BeginRead(block, AsyncReaderParameters{.path = BlockPath(block_manager->parent_path(), *block),
                                                              .record_index = record_index,
                                                              .chunk_size = kDefaultMaxReadChunk,
                                                              .time = ToTimePoint(get_timestamp(record_index))});
    RETURN_ERROR(reader_err)

    NextRecord next_record{
        .reader = reader,
        .last = last,
    };

    return next_record;
  }

  [[nodiscard]] bool is_outdated() const override {
    const auto current_time = Time::clock::now();
    return current_time - info_.last_update > info_.options.ttl;
  }

 private:
  QueryInfo info_;
};

Result<IQuery::UPtr> IQuery::Build(const std::optional<core::Time>& start, const std::optional<core::Time>& stop,
                                   const IQuery::Options& options) {
  return {
      std::make_unique<HistoricalQuery>(start ? *start : Time::min(), stop ? *stop : Time::max(), options),
      Error::kOk,
  };
}
}  // namespace reduct::storage::query
