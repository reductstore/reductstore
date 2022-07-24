// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_IO_ASYNC_READER_H
#define REDUCT_STORAGE_IO_ASYNC_READER_H

#include <filesystem>

#include "reduct/async/io.h"
#include "reduct/proto/storage/entry.pb.h"

namespace reduct::storage::io {

struct AsyncReaderParameters {
  std::filesystem::path path;
  int record_index;
  size_t chunk_size;
};

async::IAsyncReader::UPtr BuildAsyncReader(const proto::Block& block, AsyncReaderParameters parameters);

}  // namespace reduct::storage::io

#endif  // REDUCT_STORAGE_IO_ASYNC_READER_H
