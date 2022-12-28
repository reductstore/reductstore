// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_IO_ASYNC_READER_H
#define REDUCT_STORAGE_IO_ASYNC_READER_H

#include <filesystem>

#include "reduct/async/io.h"
#include "reduct/core/time.h"
#include "reduct/proto/storage/entry.pb.h"

namespace reduct::storage::io {

struct AsyncReaderParameters {
  std::filesystem::path path;
  int record_index;
  size_t chunk_size;
  core::Time time;
};

async::IAsyncReader::UPtr BuildAsyncReader(const proto::Block& block, AsyncReaderParameters parameters);

}  // namespace reduct::storage::io

#endif  // REDUCT_STORAGE_IO_ASYNC_READER_H
