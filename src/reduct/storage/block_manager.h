// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_BLOCK_MANAGER_H
#define REDUCT_STORAGE_BLOCK_MANAGER_H

#include <google/protobuf/timestamp.pb.h>

#include <filesystem>

#include "reduct/core/error.h"
#include "reduct/proto/storage/entry.pb.h"

namespace reduct::storage {

static constexpr std::string_view kBlockExt = ".blk";
static constexpr std::string_view kMetaExt = ".meta";

class IBlockManager {
 public:
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

/**
 * Load a block by ts
 * @param parent
 * @param proto_ts timestamp (it is used for file name)
 * @param block loaded block
 * @return
 */
core::Error LoadBlockByTimestamp(std::filesystem::path parent, const google::protobuf::Timestamp& proto_ts,
                                 proto::Block* block);

/**
 * Save block to filesystem
 * @param parent
 * @param block
 * @return
 */
core::Error SaveBlock(const std::filesystem::path& parent, const proto::Block& block);

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_X_H
