// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_SERVER_API_H
#define REDUCT_STORAGE_SERVER_API_H

#include "reduct/api/api.h"
#include "reduct/storage/storage.h"

namespace reduct::api {

class ServerApi {
 public:
  /**
   * GET /alive
   */
  static core::Result<HttpResponse> Alive(const storage::IStorage* storage);

  /**
   * GET /info
   */
  static core::Result<HttpResponse> Info(const storage::IStorage* storage);

  /**
   * GET /list
   */
  static core::Result<HttpResponse> List(const storage::IStorage* storage);
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_SERVER_API_H
