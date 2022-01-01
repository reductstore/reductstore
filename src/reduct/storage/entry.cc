// Copyright 2022 Alexey Timin

#include "reduct/storage/entry.h"

#include <fmt/core.h>
#include <google/protobuf/util/time_util.h>

#include <filesystem>
#include <fstream>

#include "reduct/core/logger.h"
#include "reduct/proto/entry.pb.h"

namespace reduct::storage {

using core::Error;
using google::protobuf::util::TimeUtil;
namespace fs = std::filesystem;

class Entry : public IEntry {
 public:
  /**
   * Create a new entry
   * @param options
   */
  explicit Entry(Options options) : options_(std::move(options)) {
    full_path_ = options_.path / options.name;
    proto::EntrySettings settings;
    settings.set_min_block_size(options_.min_block_size);

    auto settings_path = full_path_ / kSettingsName;
    std::ofstream settings_file(settings_path);
    if (settings_file) {
      settings.SerializeToOstream(&settings_file);
    } else {
      throw std::runtime_error(
          fmt::format("Initialization error: Failed to save settings to {}", settings_path.string()));
    }

    descriptor_.mutable_created_at()->CopyFrom(TimeUtil::GetCurrentTime());
    descriptor_.set_size(0);

    current_block_ = descriptor_.add_blocks();
    current_block_->set_id(0);
    current_block_->set_size(0);

    auto descriptor_path = full_path_ / kDescriptorName;
    std::ofstream descriptor_file(descriptor_path);
    if (descriptor_file) {
      descriptor_.SerializeToOstream(&descriptor_file);
    } else {
      throw std::runtime_error(
          fmt::format("Initialization error: Failed to save descriptor to {}", descriptor_path.string()));
    }
  }

  [[nodiscard]] Error Write(std::string&& blob, const Time& time) override {
    const auto proto_ts = FromTimePoint(time);
    const auto block_path = full_path_ / fmt::format("{:08d}", current_block_->id());

    LOG_DEBUG("Write a record_entry for time={} ({} kB) to {}", TimeUtil::ToString(proto_ts), blob.size() / 1024,
              block_path.string());

    if (!current_block_->has_first_record_time()) {
      LOG_DEBUG("First record_entry for current block");
      current_block_->mutable_first_record_time()->CopyFrom(proto_ts);
      if (current_block_->id() == 0) {
        descriptor_.mutable_first_record_time()->CopyFrom(proto_ts);
      }
    }

    // TODO(Alexey Timin): the time maybe less then first_record_time. but we write it anyway

    std::ofstream block_file(block_path, std::ios::app);
    if (!block_file) {
      return {.code = 500, .message = "Failed open a block_file for writing"};
    }

    proto::EntryRecord record_entry;
    record_entry.mutable_timestamp()->CopyFrom(proto_ts);
    record_entry.set_blob(std::move(blob));

    std::string data;
    if (!record_entry.SerializeToString(&data)) {
      return {.code = 500, .message = "Failed write a record_entry to a block_file"};
    }

    block_file << data;

    auto record = current_block_->add_records();
    record->mutable_timestamp()->CopyFrom(proto_ts);
    record->set_begin(current_block_->size());
    record->set_end(current_block_->size() + data.size());

    current_block_->set_size(current_block_->size() + data.size());
    current_block_->mutable_last_record_time()->CopyFrom(proto_ts);

    descriptor_.set_size(descriptor_.size() + data.size());
    descriptor_.mutable_last_record_time()->CopyFrom(proto_ts);

    auto descriptor_path = full_path_ / kDescriptorName;
    std::ofstream descriptor_file(descriptor_path);
    if (descriptor_file) {
      descriptor_.SerializeToOstream(&descriptor_file);
    } else {
      return {.code = 500, .message = "Failed save a descriptor"};
    }

    // TODO(Alexey Timin): create a new block when the current is too big
    return {};
  }

  [[nodiscard]] ReadResult Read(const Time& time) const override {
    const auto proto_ts = FromTimePoint(time);

    LOG_DEBUG("Read a record for time={}", TimeUtil::ToString(proto_ts));

    if (proto_ts < descriptor_.first_record_time() || proto_ts > descriptor_.last_record_time()) {
      return {{}, {.code = 404, .message = "No records for this timestamp"}, time};
    }

    auto block_index = -1;
    for (auto i = 0; i < descriptor_.blocks_size(); ++i) {
      const auto& current_block = descriptor_.blocks(i);
      if (proto_ts >= current_block.first_record_time() || proto_ts <= current_block.last_record_time()) {
        block_index = i;
        break;
      }
    }

    if (block_index == -1) {
      return {{}, {.code = 500, .message = "Failed to find the needed block in descriptor"}, time};
    }

    auto record_index = -1;
    const auto& block = descriptor_.blocks(block_index);
    for (int i = 0; i < block.records_size(); ++i) {
      const auto &current_record = block.records(i);
      if (current_record.timestamp() == proto_ts) {
        record_index = i;
        break;
      }
    }

    if (record_index == -1) {
      return {{}, {.code = 404, .message = "No records for this timestamp"}, time};
    }

    const auto block_path = full_path_ / fmt::format("{:08d}", block.id());
    LOG_DEBUG("Found block {} with needed record", block_path.string());

    std::ifstream block_file(block_path);
    if (!block_file) {
      return {{}, {.code = 500, .message = "Failed open a block for reading"}, time};
    }

    auto record = block.records(record_index);
    auto data_size = record.end() - record.begin();
    std::string data("", data_size);
    block_file.seekg(record.begin());
    block_file.read(data.data(), data_size);

    proto::EntryRecord entry_record;
    if (!entry_record.ParseFromString(data)) {
      return {{}, {.code = 500, .message = "Failed parse a block"}, time};
    }

    if (entry_record.timestamp() != proto_ts) {
      return {{}, {.code = 500, .message = "Failed to find the needed data in block"}, time};
    }

    return {entry_record.blob(), {}, time};
  }

 private:
  static google::protobuf::Timestamp FromTimePoint(const Time& time) {
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time.time_since_epoch()).count();
    return TimeUtil::MicrosecondsToTimestamp(microseconds);
  }

  static constexpr std::string_view kSettingsName = "entry.settings";
  static constexpr std::string_view kDescriptorName = "entry.descriptor";

  Options options_;
  fs::path full_path_;
  proto::EntryDescriptor descriptor_;
  proto::EntryDescriptor::Block* current_block_;
};

std::unique_ptr<IEntry> IEntry::Build(IEntry::Options options) { return std::make_unique<Entry>(std::move(options)); }

};  // namespace reduct::storage