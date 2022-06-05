// Copyright 2022 Alexey Timin

#include "reduct/storage/block_manager.h"

#include <fcntl.h>
#include <fmt/core.h>
#include <google/protobuf/util/time_util.h>

#include <fstream>
#include <utility>

namespace reduct::storage {

namespace fs = std::filesystem;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

using core::Error;

std::filesystem::path BlockPath(const fs::path& parent, const proto::Block& block, std::string_view ext) {
  auto block_path = parent / fmt::format("{}{}", TimeUtil::TimestampToMicroseconds(block.begin_time()), ext);
  return block_path;
}

class BlockManager : public IBlockManager {
 public:
  explicit BlockManager(fs::path parent) : parent_(std::move(parent)) {}

  core::Result<BlockSPtr> LoadBlock(const Timestamp& proto_ts) override {
    if (latest_loaded_ && latest_loaded_->begin_time() == proto_ts) {
      return {latest_loaded_, Error::kOk};
    }

    auto file_name = parent_ / fmt::format("{}{}", TimeUtil::TimestampToMicroseconds(proto_ts), kMetaExt);
    std::ifstream file(file_name);
    if (!file) {
      return {
          nullptr,
          {
              .code = 500,
              .message =
                  fmt::format("Failed to load a block descriptor {}: {}", file_name.string(), std::strerror(errno)),
          },
      };
    }

    latest_loaded_ = std::make_shared<proto::Block>();
    if (!latest_loaded_->ParseFromIstream(&file)) {
      latest_loaded_ = nullptr;
      return {nullptr, {.code = 500, .message = fmt::format("Failed to parse meta: {}", file_name.string())}};
    }

    return {latest_loaded_, Error::kOk};
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
        return {{}, {.code = 500, .message = ec.message()}};
      }
    } else {
      return {{}, {.code = 500, .message = strerror(errno)}};
    }

    auto err = SaveBlock(latest_loaded_);
    if (err) {
      return {{}, err};
    }

    return {latest_loaded_, Error::kOk};
  }

  core::Error SaveBlock(const BlockSPtr& block) const override {
    auto block_path = BlockPath(parent_, *block, kMetaExt);
    std::ofstream file(block_path);
    if (file) {
      block->SerializeToOstream(&file);
      return {};
    } else {
      return {.code = 500, .message = "Failed to save a block descriptor"};
    }
  }

  Error FinishBlock(const BlockSPtr& block) const override {
    auto block_path = BlockPath(parent_, *block, kBlockExt);
    std::error_code ec;
    fs::resize_file(block_path, block->size(), ec);
    if (ec) {
      return {.code = 500, .message = ec.message()};
    }

    return Error::kOk;
  }

  Error RemoveBlock(const BlockSPtr& block) override {
    std::error_code ec;

    const auto& readers = RemoveDeadReaders(block);
    if (!readers.empty()) {
      return {.code = 500, .message = "Block has active readers"};
    }

    fs::remove(BlockPath(parent_, *block), ec);
    if (ec) {
      return {.code = 500, .message = ec.message()};
    }

    fs::remove(BlockPath(parent_, *block, kMetaExt), ec);
    if (ec) {
      return {.code = 500, .message = ec.message()};
    }

    return Error::kOk;
  }

  core::Result<async::IAsyncReader::SPtr> BeginRead(const BlockSPtr& block, AsyncReaderParameters params) override {
    std::shared_ptr<async::IAsyncReader> reader = BuildAsyncReader(*block, std::move(params));

    auto& readers = RemoveDeadReaders(block);
    readers.push_back(reader);

    return {reader, Error::kOk};
  }

 private:
  std::vector<std::weak_ptr<async::IAsyncReader>>& RemoveDeadReaders(const BlockSPtr& block) {
    auto& readers = current_readers_[block->begin_time()];
    std::erase_if(readers, [](auto reader) { return !reader.lock() || reader.lock()->is_done(); });
    return readers;
  }

  fs::path parent_;
  BlockSPtr latest_loaded_;
  std::map<Timestamp, std::vector<std::weak_ptr<async::IAsyncReader>>> current_readers_;
};

std::unique_ptr<IBlockManager> IBlockManager::Build(const std::filesystem::path& parent) {
  return std::make_unique<BlockManager>(parent);
}
}  // namespace reduct::storage
