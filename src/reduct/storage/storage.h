// Copyright 2021 Alexey Timin
#ifndef REDUCT_STORAGE_STORAGE_H
#define REDUCT_STORAGE_STORAGE_H

#include "reduct/api/api_server.h"

namespace reduct::storage {

/**
 * Data Storage
 */
class IStorage {
 public:
  struct Options {};

  /**
   * Binds storage with HTT API
   * @return pointer to the handler with all the callbacks
   */
  [[nodiscard]] virtual std::unique_ptr<api::IApiHandler> BindWithApi() const = 0;

  /**
   * Build storage
   * @param options
   * @return
   */
  static std::unique_ptr<IStorage> Build(Options options);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_STORAGE_H
