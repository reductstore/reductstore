// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_API_SERVER_H
#define REDUCT_STORAGE_API_SERVER_H

#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "reduct/api/callbacks.h"
#include "reduct/asset/asset_manager.h"
#include "reduct/auth/token_auth.h"
#include "reduct/core/error.h"

namespace reduct::api {

/**
 * API handler with all the request callbacks
 */
class IApiHandler : public IInfoCallback,
                    public IListStorageCallback,
                    public ICreateBucketCallback,
                    public IGetBucketCallback,
                    public IRemoveBucketCallback,
                    public IUpdateBucketCallback,
                    public IWriteEntryCallback,
                    public IReadEntryCallback,
                    public IListEntryCallback,
                    public IQueryCallback {};

/**
 * HTTP API Server
 */
class IApiServer {
 public:
  /**
   * Components of the API server
   */
  struct Components {
    std::unique_ptr<IApiHandler> storage;
    std::unique_ptr<auth::ITokenAuthentication> auth;
    std::unique_ptr<asset::IAssetManager> console;
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
  static std::unique_ptr<IApiServer> Build(Components components, Options options);

  /**
   * Runs HTTP server
   */
  virtual int Run(const bool &running) const = 0;
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_API_SERVER_H
