// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_BLOCK_MANAGER_H
#define REDUCT_STORAGE_BLOCK_MANAGER_H

#include <google/protobuf/timestamp.pb.h>

#include <filesystem>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/storage/entry.pb.h"

namespace reduct::storage {

static constexpr std::string_view kBlockExt = ".blk";
static constexpr std::string_view kMetaExt = ".meta";

class IBlockManager {
 public:
  virtual ~IBlockManager() = default;
  using BlockSPtr = std::shared_ptr<proto::Block>;

  virtual core::Result<BlockSPtr> LoadBlock(const google::protobuf::Timestamp& proto_ts) = 0;
  virtual core::Result<BlockSPtr> StartBlock(const google::protobuf::Timestamp& proto_ts, size_t max_block_size) = 0;
  virtual core::Error SaveBlock(const BlockSPtr& block) const = 0;

  static std::unique_ptr<IBlockManager> Build(const std::filesystem::path& parent);
};

/**
 * Builds a full path for a data block or meta block
 * @note it uses Block::begin_time for file name
 * @param parent
 * @param block
 * @param ext type of block
 * @return
 */
std::filesystem::path BlockPath(const std::filesystem::path& parent, const proto::Block& block,
                                std::string_view ext = kBlockExt);

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_X_H
