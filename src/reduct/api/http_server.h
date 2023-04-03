// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_HTTP_SERVER_H
#define REDUCT_STORAGE_HTTP_SERVER_H

#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "reduct/auth/token_auth.h"
#include "reduct/auth/token_repository.h"
#include "reduct/core/error.h"
#include "reduct/storage/storage.h"
#include "rust/rust_part.h"

namespace reduct::api {

/**
 * HTTP API Server
 */
class IHttpServer {
 public:
  /**
   * Components of the API server
   */
  struct Components {
    std::unique_ptr<storage::IStorage> storage;
    std::unique_ptr<auth::ITokenAuthorization> auth;
    std::unique_ptr<auth::ITokenRepository> token_repository;
    rust::Box<reduct::asset::ZipAssetManager> console;
  };

  struct Options {
    std::string host;
    int port;
    std::string base_path;
    std::string cert_path;
    std::string cert_key_path;
  };

  /**
   * Build implementation of API Server
   * @param handler Handler with all the callbacks to process HTTP request separately
   * @param options
   * @return pointer to the implementation
   */
  static std::unique_ptr<IHttpServer> Build(Components components, Options options);

  /**
   * Runs HTTP server
   */
  virtual int Run(const bool &running) const = 0;
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_HTTP_SERVER_H
