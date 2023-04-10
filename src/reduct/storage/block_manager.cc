// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/storage/block_manager.h"

#include <fcntl.h>
#include <fmt/core.h>
#include <google/protobuf/util/time_util.h>

#include <fstream>
#include <utility>

#include "reduct/core/logger.h"

namespace reduct::storage {

namespace fs = std::filesystem;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

using core::Error;
using io::AsyncReaderParameters;
using io::AsyncWriterParameters;

std::filesystem::path BlockPath(const fs::path& parent, const proto::Block& block, std::string_view ext) {
  auto block_path = parent / fmt::format("{}{}", TimeUtil::TimestampToMicroseconds(block.begin_time()), ext);
  return block_path;
}

class BlockManager : public IBlockManager {
 public:
  explicit BlockManager(fs::path parent) : parent_(std::move(parent)) {}

  core::Result<BlockSPtr> LoadBlock(const Timestamp& proto_ts) override {
    if (latest_loaded_ && latest_loaded_->begin_time() == proto_ts) {
      return latest_loaded_;
    }

    auto file_name = parent_ / fmt::format("{}{}", TimeUtil::TimestampToMicroseconds(proto_ts), kMetaExt);
    std::ifstream file(file_name);
    if (!file) {
      return Error::InternalError(fmt::format("Failed to open file {}: {}", file_name.string(), std::strerror(errno)));
    }

    latest_loaded_ = std::make_shared<proto::Block>();
    if (!latest_loaded_->ParseFromIstream(&file)) {
      latest_loaded_ = nullptr;
      return Error::InternalError(fmt::format("Failed to parse meta: {}", file_name.string()));
    }

    return latest_loaded_;
  }

  core::Result<BlockSPtr> StartBlock(const Timestamp& proto_ts, size_t max_block_size) override {
    // allocate the whole block
    latest_loaded_ = std::make_shared<proto::Block>();
    latest_loaded_->mutable_begin_time()->CopyFrom(proto_ts);

    auto block_path = BlockPath(parent_, *latest_loaded_, kBlockExt);

    if (auto file = std::ofstream(block_path, std::ios::binary)) {
      std::error_code ec;
      fs::resize_file(block_path, max_block_size, ec);
      if (ec) {
        return Error::InternalError(ec.message());
      }
    } else {
      return Error::InternalError(strerror(errno));
    }

    RETURN_ERROR(SaveBlock(latest_loaded_));
    return latest_loaded_;
  }

  core::Error SaveBlock(const BlockSPtr& block) const override {
    auto block_path = BlockPath(parent_, *block, kMetaExt);
    std::ofstream file(block_path);
    if (file) {
      block->SerializeToOstream(&file);
      return Error::kOk;
    } else {
      return Error::InternalError("Failed to save a block descriptor");
    }
  }

  Error FinishBlock(const BlockSPtr& block) const override {
    auto block_path = BlockPath(parent_, *block, kBlockExt);
    std::error_code ec;
    fs::resize_file(block_path, block->size(), ec);
    if (ec) {
      return Error::InternalError(ec.message());
    }

    return Error::kOk;
  }

  Error RemoveBlock(const BlockSPtr& block) override {
    const auto& readers = RemoveDeadReaders(block);
    if (!readers.empty()) {
      return Error::InternalError("Block has active readers");
    }

    const auto& writers = RemoveDeadWriters(block);
    if (!writers.empty()) {
      return Error::InternalError("Block has active writers");
    }

    std::error_code ec;
    auto path = BlockPath(parent_, *block);
    auto make_error = [&ec, &path] {
      return Error::InternalError(fmt::format("Failed to remove block {}: {}", path.string(), ec.message()));
    };

    // remove block with data
    fs::remove(path, ec);
    Error err;
    if (ec) {
      err = make_error();
      LOG_WARNING(err.ToString());
    }

    // remove descriptor
    path = BlockPath(parent_, *block, kMetaExt);
    fs::remove(path, ec);
    if (ec) {
      err = make_error();
      LOG_WARNING(err.ToString());
    }

    return err;
  }

  core::Result<async::IAsyncReader::SPtr> BeginRead(const BlockSPtr& block, AsyncReaderParameters params) override {
    async::IAsyncReader::SPtr reader = BuildAsyncReader(*block, std::move(params));

    auto& readers = RemoveDeadReaders(block);
    readers.push_back(reader);

    return reader;
  }

  core::Result<async::IAsyncWriter::SPtr> BeginWrite(const BlockSPtr& block, AsyncWriterParameters params) override {
    async::IAsyncWriter::SPtr writer =
        BuildAsyncWriter(*block, std::move(params), [this, ts = block->begin_time()](int index, auto state) {
          auto [blk, load_err] = LoadBlock(ts);
          if (load_err) {
            LOG_ERROR("{}", load_err.ToString());
            return;
          }

          blk->mutable_records(index)->set_state(state);
          blk->set_invalid(state == proto::Record::kInvalid);

          if (auto err = SaveBlock(blk)) {
            LOG_ERROR("{}", err.ToString());
          }
        });

    auto& writers = RemoveDeadWriters(block);
    writers.push_back(writer);

    return writer;
  }

  const std::filesystem::path& parent_path() const override { return parent_; }

 private:
  std::vector<std::weak_ptr<async::IAsyncReader>>& RemoveDeadReaders(const BlockSPtr& block) {
    auto& readers = current_readers_[block->begin_time()];
    std::erase_if(readers, [](auto reader) { return !reader.lock() || reader.lock()->is_done(); });
    return readers;
  }

  std::vector<std::weak_ptr<async::IAsyncWriter>>& RemoveDeadWriters(const BlockSPtr& block) {
    auto& writers = current_writers_[block->begin_time()];
    std::erase_if(writers, [](auto reader) { return !reader.lock() || reader.lock()->is_done(); });
    return writers;
  }

  fs::path parent_;
  BlockSPtr latest_loaded_;
  std::map<Timestamp, std::vector<std::weak_ptr<async::IAsyncReader>>> current_readers_;
  std::map<Timestamp, std::vector<std::weak_ptr<async::IAsyncWriter>>> current_writers_;
};

std::unique_ptr<IBlockManager> IBlockManager::Build(const std::filesystem::path& parent) {
  return std::make_unique<BlockManager>(parent);
}
}  // namespace reduct::storage
