// Copyright 2021 Alexey Timin
#ifndef REDUCT_STORAGE_STORAGE_H
#define REDUCT_STORAGE_STORAGE_H

#include <filesystem>

#include "reduct/api/callbacks.h"
#include "reduct/proto/api/server.pb.h"

namespace reduct::storage {

/**
 * Data Storage
 */
class IStorage : public api::IApiHandler {
 public:
  struct Options {
    std::filesystem::path data_path;
  };

  virtual ~IStorage() = default;

  /**
   * Returns statistics about storage
   * @return
   */
  [[nodiscard]] virtual core::Result<proto::api::ServerInfo> GetInfo() const = 0;

  /**
   * Build storage
   * @param options
   * @return
   */
  static std::unique_ptr<IStorage> Build(Options options);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_STORAGE_H
