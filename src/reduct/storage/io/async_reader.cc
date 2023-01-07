// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "async_reader.h"

#include <google/protobuf/util/time_util.h>

#include <algorithm>
#include <fstream>

#include "reduct/core/logger.h"

namespace reduct::storage::io {

using core::Error;
using google::protobuf::util::TimeUtil;

class AsyncReader : public async::IAsyncReader {
 public:
  AsyncReader(const proto::Block& block, AsyncReaderParameters parameters)
      : parameters_(std::move(parameters)), size_{}, read_bytes_{} {
    file_ = std::ifstream(parameters_.path, std::ios::binary);

    const auto& record = block.records(parameters_.record_index);
    file_.seekg(record.begin());
    size_ = record.end() - record.begin();

    for (const auto& label : record.labels()) {
      labels_.insert({label.name(), label.value()});
    }
  }

  ~AsyncReader() override = default;

  core::Result<DataChunk> Read() noexcept override {
    DataChunk chunk;
    if (!file_) {
      return {chunk, Error::InternalError("Bad block")};
    }

    chunk.data.resize(std::min(parameters_.chunk_size, size_ - read_bytes_));
    file_.read(chunk.data.data(), chunk.data.size());
    read_bytes_ += chunk.data.size();

    chunk.last = size_ == read_bytes_;

    return {chunk, Error::kOk};
  }

  size_t size() const noexcept override { return size_; }

  bool is_done() const noexcept override { return size_ == read_bytes_; }

  core::Time timestamp() const noexcept override { return parameters_.time; }

  const std::map<std::string, std::string>& labels() const noexcept override { return labels_; }

 private:
  AsyncReaderParameters parameters_;
  size_t size_;
  size_t read_bytes_;
  std::ifstream file_;
  std::map<std::string, std::string> labels_;
};

async::IAsyncReader::UPtr BuildAsyncReader(const proto::Block& block, AsyncReaderParameters parameters) {
  return std::make_unique<AsyncReader>(block, std::move(parameters));
}

}  // namespace reduct::storage::io
