// Copyright 2022 Alexey Timin

#include "async_reader.h"

#include <fstream>

#include "reduct/core/logger.h"

namespace reduct::storage::io {

using core::Error;

class AsyncReader : public async::IAsyncReader {
 public:
  AsyncReader(const proto::Block& block, AsyncReaderParameters parameters)
      : parameters_(std::move(parameters)), size_{}, read_bytes_{} {
    file_ = std::ifstream(parameters_.path, std::ios::binary);

    const auto& record = block.records(parameters_.record_index);
    file_.seekg(record.begin());
    size_ = record.end() - record.begin();
  }

  ~AsyncReader() override = default;

  core::Result<DataChunk> Read() noexcept override {
    DataChunk chunk;
    if (!file_) {
      return {chunk, {.code = 500, .message = "Bad block"}};
    }

    chunk.data.resize(std::min(parameters_.chunk_size, size_ - read_bytes_));
    file_.read(chunk.data.data(), chunk.data.size());
    read_bytes_ += chunk.data.size();

    chunk.last = size_ == read_bytes_;

    return {chunk, Error::kOk};
  }

  size_t size() const noexcept override { return size_; }
  bool is_done() const noexcept override { return size_ == read_bytes_; }

 private:
  AsyncReaderParameters parameters_;
  size_t size_;
  size_t read_bytes_;
  std::ifstream file_;
};

async::IAsyncReader::UPtr BuildAsyncReader(const proto::Block& block, AsyncReaderParameters parameters) {
  return std::make_unique<AsyncReader>(block, std::move(parameters));
}

}  // namespace reduct::storage::io
