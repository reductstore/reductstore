// Copyright 2022 Alexey Timin

#include "reduct/storage/block_helpers.h"

#include <fmt/core.h>
#include <google/protobuf/util/time_util.h>

#include <fstream>

namespace reduct::storage {

namespace fs = std::filesystem;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

using core::Error;

std::filesystem::path BlockPath(const fs::path& parent, const proto::Block& block, std::string_view ext) {
  auto block_path = parent / fmt::format("{}{}", TimeUtil::TimestampToMicroseconds(block.begin_time()), ext);
  return block_path;
}

Error LoadBlockByTimestamp(fs::path folder, const Timestamp& proto_ts, proto::Block* block) {
  auto file_name = folder / fmt::format("{}{}", TimeUtil::TimestampToMicroseconds(proto_ts), kMetaExt);
  std::ifstream file(file_name);
  if (!file) {
    return {.code = 500,
            .message = fmt::format("Failed to load a block descriptor {}: {}", file_name.string(),
            std::strerror(errno))};
  }

  if (!block->ParseFromIstream(&file)) {
    return {.code = 500, .message = fmt::format("Failed to parse meta: {}", file_name.string())};
  }
  return Error::kOk;
}

Error SaveBlock(const fs::path& parent, const proto::Block& block) {
  auto file_name = BlockPath(parent, block, kMetaExt);
  std::ofstream file(file_name);
  if (file) {
    block.SerializeToOstream(&file);
    return {};
  } else {
    return {.code = 500, .message = "Failed to save a block descriptor"};
  }
}

}  // namespace reduct::storage
