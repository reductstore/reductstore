// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_API_SERVER_H
#define REDUCT_STORAGE_API_SERVER_H

#include <map>
#include <memory>
#include <string>

#include "reduct/core/error.h"

namespace reduct::api {

class IApiHandler {
 public:
  struct InfoResponse {
    std::string version;
  };

  [[nodiscard]] virtual core::Error OnInfoRequest(InfoResponse* info) const = 0;
};

class IApiServer {
 public:
  struct Options {
    std::string host;
    int port;
    std::string base_path;
  };

  static std::unique_ptr<IApiServer> Build(std::unique_ptr<IApiHandler> handler, Options options);

  /**
   * Runs HTTP server
   */
  virtual void Run() const = 0;
};

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_API_SERVER_H
