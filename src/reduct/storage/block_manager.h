// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_BLOCK_MANAGER_H
#define REDUCT_STORAGE_BLOCK_MANAGER_H

#include <google/protobuf/timestamp.pb.h>

#include <filesystem>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/storage/entry.pb.h"
#include "reduct/storage/io/async_reader.h"
#include "reduct/storage/io/async_writer.h"

namespace reduct::storage {

static constexpr std::string_view kBlockExt = ".blk";
static constexpr std::string_view kMetaExt = ".meta";

/**
 * Creates, loads removes blocks of data
 * @note It has to be only one in the app, because caches the last loaded block
 */
class IBlockManager {
 public:
  using BlockSPtr = std::shared_ptr<proto::Block>;

  virtual ~IBlockManager() = default;

  /**
   * Load a block and save in a cache
   * @param proto_ts
   * @return
   */
  virtual core::Result<BlockSPtr> LoadBlock(const google::protobuf::Timestamp& proto_ts) = 0;

  /**
   * Starts a new block and save it a cache
   * @param proto_ts
   * @param max_block_size
   * @return
   */
  virtual core::Result<BlockSPtr> StartBlock(const google::protobuf::Timestamp& proto_ts, size_t max_block_size) = 0;

  /**
   * Save a block to filesystem
   * @param block
   * @return
   */
  virtual core::Error SaveBlock(const BlockSPtr& block) const = 0;

  /**
   * Finish a block
   * @param block
   * @return
   */
  virtual core::Error FinishBlock(const BlockSPtr& block) const = 0;

  /**
   * Remove block from disk
   * @param block
   * @return
   */
  virtual core::Error RemoveBlock(const BlockSPtr& block) = 0;

  /**
   * Begin reading a block
   * @note it stores weak pointers to the readers to keep track of them
   * @param block
   * @param params
   * @return
   */
  virtual core::Result<async::IAsyncReader::SPtr> BeginRead(const BlockSPtr& block,
                                                            io::AsyncReaderParameters params) = 0;

  virtual core::Result<async::IAsyncWriter::SPtr> BeginWrite(const BlockSPtr& block,
                                                             io::AsyncWriterParameters params) = 0;

  /**
   * Factory method
   * @param parent
   * @return
   */
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
