// Copyright 2022 Alexey Timin

#include "reduct/storage/async_writer.h"

#include <filesystem>
#include <fstream>
#include <utility>

#include "reduct/core/logger.h"
#include "reduct/storage/block_helpers.h"

namespace reduct::storage {

using core::Error;
using google::protobuf::Timestamp;

namespace fs = std::filesystem;

/**
 * @class Asynchronous writer
 * @brief Writes chunks of data into pre-allocated block
 */
class AsyncWriter : public async::IAsyncWriter {
 public:
  AsyncWriter(const proto::Block& block, AsyncWriterParameters parameters)
      : ts_(block.begin_time()), parameters_(std::move(parameters)), writen_size_{} {
    file_ = std::ofstream(parameters_.path, std::ios::out | std::ios::in | std::ios::binary);
    file_.seekp(block.records(parameters_.record_index).begin());
  }

  Error Write(std::string_view chunk, bool last) noexcept override {
    if (!file_) {
      UpdateRecord(proto::Record::kErrored);
      return {.code = 500, .message = "Bad block"};
    }

    writen_size_ += chunk.size();
    if (writen_size_ > parameters_.size) {
      UpdateRecord(proto::Record::kErrored);
      return {.code = 413, .message = "Content is bigger than in content-length"};
    }

    if (!file_.write(chunk.data(), chunk.size())) {
      UpdateRecord(proto::Record::kErrored);
      return {.code = 500, .message = "Failed to write a chunk into a block"};
    }

    if (last) {
      UpdateRecord(proto::Record::kFinished);
      file_ << std::flush;
    }

    return Error::kOk;
  }

 private:
  void UpdateRecord(proto::Record::State state) {
    proto::Block block;
    if (auto err = LoadBlockByTimestamp(parameters_.path.parent_path(), ts_, &block)) {
      LOG_ERROR("{}", err);
      return;
    }

    block.mutable_records(parameters_.record_index)->set_state(state);
    if (auto err = SaveBlock(parameters_.path.parent_path(), block)) {
      LOG_ERROR("{}", err);
    }
  }

  std::ofstream file_;
  Timestamp ts_;
  AsyncWriterParameters parameters_;
  size_t writen_size_;
};

async::IAsyncWriter::UPtr BuildAsyncWriter(const proto::Block& block, AsyncWriterParameters parameters) {
  return std::make_unique<AsyncWriter>(block, std::move(parameters));
}
}  // namespace reduct::storage
