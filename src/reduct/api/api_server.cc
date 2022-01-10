// Copyright 2021-2022 Alexey Timin

#include "reduct/api/api_server.h"

#include <App.h>
#include <nlohmann/json.hpp>

#include "reduct/api/handlers/common.h"
#include "reduct/api/handlers/handle_bucket.h"
#include "reduct/api/handlers/handle_entry.h"
#include "reduct/api/handlers/handle_info.h"
#include "reduct/core/logger.h"

namespace reduct::api {

using core::Error;
using uWS::HttpRequest;
using uWS::HttpResponse;

class ApiServer : public IApiServer {
 public:
  explicit ApiServer(std::unique_ptr<IApiHandler> handler, Options options)
      : handler_(std::move(handler)), options_(std::move(options)) {}

  void Run() const override {
    const auto &[host, port, base_path] = options_;

    uWS::App()
        .get(base_path + "info", [this](auto *res, auto *req) { handlers::HandleInfo(handler_.get(), res, req); })
        // Bucket API
        .post(base_path + ":bucket_name",
              [this](auto *res, auto *req) {
                handlers::HandleCreateBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
              })
        .get(base_path + ":bucket_name",
             [this](auto *res, auto *req) {
               handlers::HandleGetBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
             })
        .put(base_path + ":bucket_name",
             [this](auto *res, auto *req) {
               handlers::HandleUpdateBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
             })
        .del(base_path + ":bucket_name",
             [this](auto *res, auto *req) {
               handlers::HandleRemoveBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
             })
        // Entry API
        .post(base_path + ":bucket_name/:entry_name",
              [this](auto *res, auto *req) {
                handlers::HandleWriteEntry(handler_.get(), res, req, std::string(req->getParameter(0)),
                                           std::string(req->getParameter(1)), std::string(req->getQuery("ts")));
              })
        .get(base_path + ":bucket_name/:entry_name",
             [this](auto *res, auto *req) {
               handlers::HandleReadEntry(handler_.get(), res, req, std::string(req->getParameter(0)),
                                         std::string(req->getParameter(1)), std::string(req->getQuery("ts")));
             })
        .listen(host, port, 0,
                [&](auto sock) {
                  if (sock) {
                    LOG_INFO("Run HTTP server on {}:{}", host, port);
                  } else {
                    LOG_ERROR("Failed to listen to {}:{}", host, port);
                  }
                })
        .run();
  }

 private:
  Options options_;
  std::unique_ptr<IApiHandler> handler_;
};

std::unique_ptr<IApiServer> IApiServer::Build(std::unique_ptr<IApiHandler> handler, Options options) {
  return std::make_unique<ApiServer>(std::move(handler), std::move(options));
}

}  // namespace reduct::api
