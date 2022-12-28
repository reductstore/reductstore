// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "async_writer.h"

#include <filesystem>
#include <fstream>
#include <utility>

#include "reduct/core/logger.h"
#include "reduct/storage/block_manager.h"

namespace reduct::storage::io {

using core::Error;
using google::protobuf::Timestamp;

namespace fs = std::filesystem;

/**
 * @class Asynchronous writer
 * @brief Writes chunks of data into pre-allocated block
 */
class AsyncWriter : public async::IAsyncWriter {
 public:
  AsyncWriter(const proto::Block& block, AsyncWriterParameters parameters, OnStateUpdated callback)
      : parameters_(std::move(parameters)), writen_size_{}, update_record_(callback) {
    file_ = std::ofstream(parameters_.path, std::ios::out | std::ios::in | std::ios::binary);
    file_.seekp(block.records(parameters_.record_index).begin());
  }

  ~AsyncWriter() override = default;

  Error Write(std::string_view chunk, bool last) noexcept override {
    const auto& record = parameters_.record_index;
    if (!file_) {
      update_record_(record, proto::Record::kInvalid);
      return Error::InternalError("Bad block");
    }

    writen_size_ += chunk.size();
    if (writen_size_ > parameters_.size) {
      update_record_(record, proto::Record::kErrored);
      return Error::BadRequest("Content is bigger than in content-length");
    }

    if (!file_.write(chunk.data(), chunk.size())) {
      update_record_(record, proto::Record::kInvalid);
      return Error::InternalError("Failed to write a chunk into a block");
    }

    if (last) {
      if (writen_size_ < parameters_.size) {
        update_record_(record, proto::Record::kErrored);
        return Error::BadRequest("Content is smaller than in content-length");
      }

      update_record_(record, proto::Record::kFinished);
      file_ << std::flush;
    }

    return Error::kOk;
  }

  bool is_done() const noexcept override { return writen_size_ == parameters_.size; }

 private:
  std::ofstream file_;
  AsyncWriterParameters parameters_;
  size_t writen_size_;
  OnStateUpdated update_record_;
};

async::IAsyncWriter::UPtr BuildAsyncWriter(const proto::Block& block, AsyncWriterParameters parameters,
                                           OnStateUpdated callback) {
  return std::make_unique<AsyncWriter>(block, std::move(parameters), std::move(callback));
}
}  // namespace reduct::storage::io
