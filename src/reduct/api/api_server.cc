// Copyright 2021-2022 Alexey Timin

#include "reduct/api/api_server.h"

#include <App.h>

#include "common.h"
#include "reduct/core/logger.h"

namespace reduct::api {

using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;

using proto::api::BucketSettings;

using uWS::HttpRequest;
using uWS::HttpResponse;

using async::VoidTask;
using auth::ITokenAuthentication;
using core::Error;

class ApiServer : public IApiServer {
 public:
  explicit ApiServer(Components components, Options options)
      : handler_(std::move(components.handler)), auth_(std::move(components.auth)), options_(std::move(options)) {}

  void Run(const bool &running) const override {
    auto [host, port, base_path] = options_;

    if (!base_path.starts_with('/')) {
      base_path = "/" + base_path;
    }
    if (!base_path.ends_with('/')) {
      base_path.push_back('/');
    }

    uWS::App()
        // Server API
        .get(base_path + "info", [this](auto *res, auto *req) { Info(res, req); })
        .get(base_path + "list", [this](auto *res, auto *req) { List(res, req); })
        // Auth API
        .post(base_path + "auth/refresh", [this](auto *res, auto *req) { RefreshToken(res, req); })
        // Bucket API
        .post(base_path + "b/:bucket_name",
              [this](auto *res, auto *req) { CreateBucket(res, req, std::string(req->getParameter(0))); })
        .get(base_path + "b/:bucket_name",
             [this](auto *res, auto *req) { GetBucket(res, req, std::string(req->getParameter(0))); })
        .head(base_path + "b/:bucket_name",
              [this](auto *res, auto *req) { HeadBucket(res, req, std::string(req->getParameter(0))); })
        .put(base_path + "b/:bucket_name",
             [this](auto *res, auto *req) { UpdateBucket(res, req, std::string(req->getParameter(0))); })
        .del(base_path + "b/:bucket_name",
             [this](auto *res, auto *req) { RemoveBucket(res, req, std::string(req->getParameter(0))); })
        // Entry API
        .post(base_path + "b/:bucket_name/:entry_name",
              [this](auto *res, auto *req) {
                WriteEntry(res, req, std::string(req->getParameter(0)), std::string(req->getParameter(1)),
                           std::string(req->getQuery("ts")));
              })
        .get(base_path + "b/:bucket_name/:entry_name",
             [this](auto *res, auto *req) {
               ReadEntry(res, req, std::string(req->getParameter(0)), std::string(req->getParameter(1)),
                         std::string(req->getQuery("ts")));
             })
        .get(base_path + "b/:bucket_name/:entry_name/list",
             [this](auto *res, auto *req) {
               ListEntry(res, req, std::string(req->getParameter(0)), std::string(req->getParameter(1)),
                         std::string(req->getQuery("start")), std::string(req->getQuery("stop")));
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
  // Server API
  /**
   * GET /info
   */
  template <bool SSL>
  VoidTask Info(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    [[maybe_unused]] auto err =
        BasicHandle<SSL, IInfoCallback>(res, req)
            .OnSuccess([](IInfoCallback::Response app_resp) { return PrintToJson(std::move(app_resp.info)); })
            .Run(co_await handler_->OnInfo({}), auth_.get());

    co_return;
  }

  /**
   * GET /list
   */
  template <bool SSL>
  VoidTask List(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    [[maybe_unused]] auto err =
        BasicHandle<SSL, IListStorageCallback>(res, req)
            .OnSuccess([](IListStorageCallback::Response app_resp) { return PrintToJson(std::move(app_resp.buckets)); })
            .Run(co_await handler_->OnStorageList({}), auth_.get());
    co_return;
  }

  // Auth API
  /**
   * POST /auth/refresh
   */
  template <bool SSL>
  VoidTask RefreshToken(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    std::string header(req->getHeader("authorization"));
    [[maybe_unused]] auto err =
        BasicHandle<SSL, IRefreshToken>(res, req)
            .OnSuccess([](IRefreshToken::Response resp) { return PrintToJson(std::move(resp)); })
            .Run(co_await auth_->OnRefreshToken(header));
    co_return;
  }

  // Bucket API
  /**
   * POST /b/:name
   */
  template <bool SSL>
  VoidTask CreateBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto basic = BasicHandle<SSL, ICreateBucketCallback>(res, req);

    auto data = co_await AsyncHttpReceiver<SSL>(res);
    BucketSettings settings;
    if (!data.empty()) {
      auto status = JsonStringToMessage(data, &settings);
      if (!status.ok()) {
        basic.SendError(
            Error{.code = 422, .message = fmt::format("Failed parse JSON data: {}", status.message().ToString())});
        co_return;
      }
    }

    ICreateBucketCallback::Request app_request{
        .bucket_name = name,
        .bucket_settings = std::move(settings),
    };

    [[maybe_unused]] auto err = basic.Run(co_await handler_->OnCreateBucket(app_request), auth_.get());
    co_return;
  }

  /**
   * GET /b/:name
   */
  template <bool SSL>
  VoidTask GetBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    IGetBucketCallback::Request app_request{.bucket_name = name};
    [[maybe_unused]] auto err =
        BasicHandle<SSL, IGetBucketCallback>(res, req)
            .OnSuccess([](IGetBucketCallback::Response resp) { return PrintToJson(resp.bucket_settings); })
            .Run(co_await handler_->OnGetBucket(app_request), auth_.get());
    co_return;
  }

  /**
   * HEAD /b/:name
   */
  template <bool SSL>
  VoidTask HeadBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    IGetBucketCallback::Request app_request{.bucket_name = name};
    [[maybe_unused]] auto err =
        BasicHandle<SSL, IGetBucketCallback>(res, req).Run(co_await handler_->OnGetBucket(app_request), auth_.get());
    co_return;
  }

  /**
   * PUT /b/:name
   */
  template <bool SSL>
  VoidTask UpdateBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto basic = BasicHandle<SSL, IUpdateBucketCallback>(res, req);

    auto data = co_await AsyncHttpReceiver<SSL>(res);
    BucketSettings settings;
    auto status = JsonStringToMessage(data, &settings);
    if (!status.ok()) {
      basic.SendError(Error{.code = 422, .message = "Failed parse JSON data"});
      co_return;
    }

    IUpdateBucketCallback::Request app_request{
        .bucket_name = name,
        .new_settings = std::move(settings),
    };
    [[maybe_unused]] auto err = basic.Run(co_await handler_->OnUpdateCallback(app_request), auth_.get());
    co_return;
  }

  /**
   * DELETE /b/:name
   */
  template <bool SSL>
  VoidTask RemoveBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    IRemoveBucketCallback::Request app_request{.bucket_name = name};
    [[maybe_unused]] auto err = BasicHandle<SSL, IRemoveBucketCallback>(res, req).Run(
        co_await handler_->OnRemoveBucket(app_request), auth_.get());
    co_return;
  }

  // Entry API
  /**
   * POST /b/:bucket/:entry
   */
  template <bool SSL>
  async::VoidTask WriteEntry(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string bucket, std::string entry,
                             std::string ts) const {
    auto basic = BasicHandle<SSL, IWriteEntryCallback>(res, req);

    auto full_blob = co_await AsyncHttpReceiver<SSL>(res);
    IWriteEntryCallback::Request data{
        .bucket_name = bucket,
        .entry_name = entry,
        .timestamp = ts,
        .blob = full_blob,
    };
    [[maybe_unused]] auto err =
        basic.Run(co_await handler_->OnWriteEntry(data), auth_.get());  // TODO(Alexey Timin): std::move crushes
    co_return;
  }

  /**
   * GET /b/:bucket/:entry
   */
  template <bool SSL = false>
  async::VoidTask ReadEntry(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string bucket, std::string entry,
                            std::string ts) const {
    IReadEntryCallback::Request data{
        .bucket_name = bucket,
        .entry_name = entry,
        .timestamp = ts,
    };

    [[maybe_unused]] auto err = BasicHandle<SSL, IReadEntryCallback>(res, req)
                                    .OnSuccess([](IReadEntryCallback::Response app_resp) { return app_resp.blob; })
                                    .Run(co_await handler_->OnReadEntry(data), auth_.get());
    co_return;
  }

  /**
   * GET /b/:bucket/:entry/list
   */
  template <bool SSL = false>
  async::VoidTask ListEntry(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string bucket, std::string entry,
                            std::string start_ts, std::string stop_ts) const {
    IListEntryCallback::Request data{
        .bucket_name = bucket,
        .entry_name = entry,
        .start_timestamp = start_ts,
        .stop_timestamp = stop_ts,
    };

    [[maybe_unused]] auto err = BasicHandle<SSL, IListEntryCallback>(res, req)
                                    .OnSuccess([](IListEntryCallback::Response app_resp) {
                                      nlohmann::json data;
                                      for (const auto &rec : app_resp.records) {
                                        nlohmann::json record;
                                        record["ts"] = rec.timestamp;
                                        record["size"] = rec.size;
                                        data["records"].push_back(record);
                                      }
                                      return data.dump();
                                    })
                                    .Run(co_await handler_->OnListEntry(data), auth_.get());
    co_return;
  }

  Options options_;
  std::unique_ptr<IApiHandler> handler_;
  std::unique_ptr<ITokenAuthentication> auth_;
};

std::unique_ptr<IApiServer> IApiServer::Build(Components components, Options options) {
  return std::make_unique<ApiServer>(std::move(components), std::move(options));
}

}  // namespace reduct::api
