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

  void Run(const bool &running) const override {
    auto [host, port, base_path] = options_;

    if (!base_path.starts_with('/')) {
      base_path = "/" + base_path;
    }
    if (!base_path.ends_with('/')) {
      base_path.push_back('/');
    }

    uWS::App()
        .get(base_path + "info", [this](auto *res, auto *req) { handlers::HandleInfo(handler_.get(), res, req); })
        // Bucket API
        .post(base_path + "b/:bucket_name",
              [this](auto *res, auto *req) {
                handlers::HandleCreateBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
              })
        .get(base_path + "b/:bucket_name",
             [this](auto *res, auto *req) {
               handlers::HandleGetBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
             })
        .put(base_path + "b/:bucket_name",
             [this](auto *res, auto *req) {
               handlers::HandleUpdateBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
             })
        .del(base_path + "b/:bucket_name",
             [this](auto *res, auto *req) {
               handlers::HandleRemoveBucket(handler_.get(), res, req, std::string(req->getParameter(0)));
             })
        // Entry API
        .post(base_path + "b/:bucket_name/:entry_name",
              [this](auto *res, auto *req) {
                handlers::HandleWriteEntry(handler_.get(), res, req, std::string(req->getParameter(0)),
                                           std::string(req->getParameter(1)), std::string(req->getQuery("ts")));
              })
        .get(base_path + "b/:bucket_name/:entry_name",
             [this](auto *res, auto *req) {
               handlers::HandleReadEntry(handler_.get(), res, req, std::string(req->getParameter(0)),
                                         std::string(req->getParameter(1)), std::string(req->getQuery("ts")));
             })
        .get(base_path + "b/:bucket_name/:entry_name/list",
             [this](auto *res, auto *req) {
               handlers::HandleListEntry(handler_.get(), res, req, std::string(req->getParameter(0)),
                                         std::string(req->getParameter(1)), std::string(req->getQuery("start")),
                                         std::string(req->getQuery("stop")));
             })
        .any("/*",
             [](auto *res, auto *req) {
               res->writeStatus("404");
               res->end({});
             })
        .listen(host, port, 0,
                [&](us_listen_socket_t *sock) {
                  if (sock) {
                    LOG_INFO("Run HTTP server on {}:{}", host, port);

                    std::thread stopper([sock, &running] {
                      // Checks running flag and closes the socket to stop the app gracefully
                      while (running) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                      }

                      LOG_INFO("Stopping storage...");
                      us_listen_socket_close(0, sock);
                    });

                    stopper.detach();

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
