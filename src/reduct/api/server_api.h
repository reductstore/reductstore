// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_SERVER_API_H
#define REDUCT_STORAGE_SERVER_API_H

#include "reduct/api/common.h"
#include "reduct/storage/storage.h"

namespace reduct::api {

class ServerApi {
 public:
  /**
   * GET /alive
   */
  static core::Result<HttpRequestReceiver> Alive(const storage::IStorage* storage);

  /**
   * GET /info
   */
  static core::Result<HttpRequestReceiver> Info(const storage::IStorage* storage);

  /**
   * GET /list
   */
  static core::Result<HttpRequestReceiver> List(const storage::IStorage* storage);
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_SERVER_API_H
