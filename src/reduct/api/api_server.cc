// Copyright 2021-2022 Alexey Timin

#include "reduct/api/api_server.h"

#include <App.h>

#include <filesystem>
#include <regex>

#include "common.h"
#include "reduct/async/sleep.h"
#include "reduct/core/logger.h"

namespace reduct::api {

using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;

using proto::api::BucketSettings;

using uWS::HttpRequest;
using uWS::HttpResponse;

using asset::IAssetManager;
using async::Sleep;
using async::Task;
using async::VoidTask;
using auth::ITokenAuthentication;
using core::Error;
using core::Result;

namespace fs = std::filesystem;

class ApiServer : public IApiServer {
 public:
  ApiServer(Components components, Options options)
      : storage_(std::move(components.storage)),
        auth_(std::move(components.auth)),
        console_(std::move(components.console)),
        options_(std::move(options)) {}

  [[nodiscard]] int Run(const bool &running) const override {
    if (options_.cert_path.empty()) {
      RegisterEndpointsAndRun(uWS::App(), running);
    } else {
      auto check_file = [](auto file) {
        if (!fs::exists(file)) {
          LOG_ERROR("File '{}' doesn't exist", file);
          return false;
        }
        return true;
      };

      if (check_file(options_.cert_path) && check_file(options_.cert_key_path)) {
        RegisterEndpointsAndRun(uWS::SSLApp(uWS::SocketContextOptions{
                                    .key_file_name = options_.cert_key_path.data(),
                                    .cert_file_name = options_.cert_path.data(),
                                }),
                                running);
      } else {
        return -1;
      }
    }

    return 0;
  }

 private:
  template <bool SSL>
  void RegisterEndpointsAndRun(uWS::TemplatedApp<SSL> &&app, const bool &running) const {
    auto [host, port, base_path, cert_path, cert_key_path] = options_;

    if (!base_path.starts_with('/')) {
      base_path = "/" + base_path;
    }
    if (!base_path.ends_with('/')) {
      base_path.push_back('/');
    }
    // Server API
    app.head(base_path + "alive", [this](auto *res, auto *req) { Alive(res, req); })
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
        .get(base_path,
             [base_path](auto *res, auto *req) {
               res->writeStatus("301");
               res->writeHeader("location", base_path + "ui/");
               res->end({});
             })
        .get(base_path + "ui/*",
             [this, base_path](auto *res, auto *req) {
               std::string path(req->getUrl());
               path = path.substr(base_path.size() + 3, path.size());

               if (path.empty()) {
                 path = "index.html";
               }

               UiRequest(res, req, base_path, path);
             })
        .any("/*",
             [](auto *res, auto *req) {
               res->writeStatus("404");
               res->end({});
             })
        .listen(host, port, 0,
                [&](us_listen_socket_t *sock) {
                  if (sock) {
                    LOG_INFO("Run HTTP server on http{}://{}:{}{}", SSL ? "s" : "", host, port, base_path);

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
  // Server API
  /**
   * HEAD /alive
   */
  template <bool SSL>
  VoidTask Alive(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    auto handler = BasicApiHandler<SSL, IInfoCallback>(res, req);
    handler.PrepareHeaders(false, "");
    res->end({});
    co_return;
  }

  /**
   * GET /info
   */
  template <bool SSL>
  VoidTask Info(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    auto handler = BasicApiHandler<SSL, IInfoCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    handler.Run(co_await storage_->OnInfo({}),
                [](const IInfoCallback::Response &app_resp) { return PrintToJson(app_resp); });
    co_return;
  }

  /**
   * GET /list
   */
  template <bool SSL>
  VoidTask List(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    auto handler = BasicApiHandler<SSL, IListStorageCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }
    handler.Run(co_await storage_->OnStorageList({}),
                [](const IListStorageCallback::Response &app_resp) { return PrintToJson(app_resp.buckets); });
    co_return;
  }

  // Auth API
  /**
   * POST /auth/refresh
   */
  template <bool SSL>
  VoidTask RefreshToken(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) const {
    std::string header(req->getHeader("authorization"));
    auto handler = BasicApiHandler<SSL, IRefreshToken>(res, req);
    handler.Run(co_await auth_->OnRefreshToken(header),
                [](const IRefreshToken::Response &resp) { return PrintToJson(resp); });
    co_return;
  }

  // Bucket API
  /**
   * POST /b/:name
   */
  template <bool SSL>
  VoidTask CreateBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto handler = BasicApiHandler<SSL, ICreateBucketCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    std::string data;
    auto err = co_await AsyncHttpReceiver<SSL>(res, [&data](auto chuck, bool _) {
      data += chuck;
      return Error::kOk;
    });
    if (err) {
      handler.SendError(err);
      co_return;
    }

    BucketSettings settings;
    if (!data.empty()) {
      auto status = JsonStringToMessage(data, &settings);
      if (!status.ok()) {
        handler.SendError(
            Error{.code = 422, .message = fmt::format("Failed parse JSON data: {}", status.message().ToString())});
        co_return;
      }
    }

    ICreateBucketCallback::Request app_request{
        .bucket_name = name,
        .bucket_settings = std::move(settings),
    };

    handler.Run(co_await storage_->OnCreateBucket(app_request));
    co_return;
  }

  /**
   * GET /b/:name
   */
  template <bool SSL>
  VoidTask GetBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto handler = BasicApiHandler<SSL, IGetBucketCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IGetBucketCallback::Request app_request{.bucket_name = name};
    handler.Run(co_await storage_->OnGetBucket(app_request),
                [](const IGetBucketCallback::Response &resp) { return PrintToJson(resp); });
    co_return;
  }

  /**
   * HEAD /b/:name
   */
  template <bool SSL>
  VoidTask HeadBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto handler = BasicApiHandler<SSL, IGetBucketCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IGetBucketCallback::Request app_request{.bucket_name = name};
    handler.Run(co_await storage_->OnGetBucket(app_request));
    co_return;
  }

  /**
   * PUT /b/:name
   */
  template <bool SSL>
  VoidTask UpdateBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto handler = BasicApiHandler<SSL, IUpdateBucketCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    std::string data;
    auto err = co_await AsyncHttpReceiver<SSL>(res, [&data](auto chuck, bool _) {
      data += chuck;
      return Error::kOk;
    });

    if (err) {
      handler.SendError(err);
      co_return;
    }

    BucketSettings settings;
    auto status = JsonStringToMessage(data, &settings);
    if (!status.ok()) {
      handler.SendError(Error{.code = 422, .message = "Failed parse JSON data"});
      co_return;
    }

    IUpdateBucketCallback::Request app_request{
        .bucket_name = name,
        .new_settings = std::move(settings),
    };
    handler.Run(co_await storage_->OnUpdateCallback(app_request));
    co_return;
  }

  /**
   * DELETE /b/:name
   */
  template <bool SSL>
  VoidTask RemoveBucket(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string name) const {
    auto handler = BasicApiHandler<SSL, IRemoveBucketCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IRemoveBucketCallback::Request app_request{.bucket_name = name};
    handler.Run(co_await storage_->OnRemoveBucket(app_request));
    co_return;
  }

  // Entry API
  /**
   * POST /b/:bucket/:entry
   */
  template <bool SSL>
  async::VoidTask WriteEntry(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string bucket, std::string entry,
                             std::string ts) const {
    auto handler = BasicApiHandler<SSL, IWriteEntryCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }
    IWriteEntryCallback::Request app_req{
        .bucket_name = bucket,
        .entry_name = entry,
        .timestamp = ts,
        .content_length = req->getHeader("content-length"),
    };

    auto [async_writer, err] = storage_->OnWriteEntry(app_req);
    if (err) {
      handler.SendError(err);
      co_return;
    }

    err = co_await AsyncHttpReceiver<SSL>(
        res, [async_writer](auto chunk, bool last) { return async_writer->Write(chunk, last); });
    if (err) {
      handler.SendError(err);
      co_return;
    }

    res->end({});
    co_return;
  }

  /**
   * GET /b/:bucket/:entry
   */
  template <bool SSL = false>
  async::VoidTask ReadEntry(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string bucket, std::string entry,
                            std::string ts) const {
    auto handler = BasicApiHandler<SSL, IReadEntryCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IReadEntryCallback::Request data{.bucket_name = bucket, .entry_name = entry, .timestamp = ts, .latest = ts.empty()};
    auto [reader, err] = co_await storage_->OnReadEntry(data);

    if (err) {
      handler.SendError(err);
      co_return;
    }

    bool ready_to_continue = false;
    res->onWritable([&ready_to_continue](auto _) {
      LOG_DEBUG("ready");
      ready_to_continue = true;
      return true;
    });

    bool aborted = false;
    res->onAborted([&aborted] {
      LOG_WARNING("aborted");
      aborted = true;
    });

    handler.PrepareHeaders(true, "application/octet-stream");

    bool complete = false;
    while (!aborted && !complete) {
      co_await Sleep(async::kTick);  // switch context before start to read
      auto [chuck, read_err] = reader->Read();
      if (read_err) {
        handler.SendError(err);
        co_return;
      }

      const auto offset = res->getWriteOffset();
      while (!aborted) {
        ready_to_continue = false;
        auto [ok, responded] = res->tryEnd(chuck.data.substr(res->getWriteOffset() - offset), reader->size());
        if (ok) {
          complete = responded;
          break;
        } else {
          // Have to wait until onWritable sets flag
          LOG_DEBUG("Failed to send data: {} {}/{} kB", ts, res->getWriteOffset() / 1024, reader->size() / 1024);
          while (!ready_to_continue && !aborted) {
            co_await Sleep(async::kTick);
          }

          continue;
        }
      }
    }

    LOG_DEBUG("Sent {} {}/{} kB", ts, res->getWriteOffset() / 1024, reader->size() / 1024);

    co_return;
  }

  /**
   * GET /b/:bucket/:entry/list
   */
  template <bool SSL = false>
  async::VoidTask ListEntry(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string bucket, std::string entry,
                            std::string start_ts, std::string stop_ts) const {
    auto handler = BasicApiHandler<SSL, IListEntryCallback>(res, req);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IListEntryCallback::Request data{
        .bucket_name = bucket,
        .entry_name = entry,
        .start_timestamp = start_ts,
        .stop_timestamp = stop_ts,
    };

    handler.Run(co_await storage_->OnListEntry(data),
                [](const IListEntryCallback::Response &app_resp) { return PrintToJson(app_resp); });
    co_return;
  }

  template <bool SSL = false>
  async::VoidTask UiRequest(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req, std::string_view base_path,
                            std::string path) const {
    struct Callback {
      struct [[maybe_unused]] Request {}; // NOLINT
      using Response = std::string;
      using Result = core::Result<Response>;
    };

    auto handler = BasicApiHandler<SSL, Callback>(res, req, "");

    auto replace_base_path = [&base_path](typename Callback::Response resp) {
      // substitute RS_API_BASE_PATH to make web console work
      resp = std::regex_replace(resp, std::regex("/ui/"), fmt::format("{}ui/", base_path));
      return resp;
    };

    auto ret = console_->Read(path);
    switch (ret.error.code) {
      case 0: {
        auto content = std::regex_replace(ret.result, std::regex("/ui/"), fmt::format("{}ui/", base_path));
        res->end(std::move(content));
        handler.Run(std::move(ret), replace_base_path);
        break;
      }
      case 404: {
        ret = console_->Read("index.html");
        handler.Run(std::move(ret), replace_base_path);
        break;
      }
      default: {
        handler.SendError(ret.error);
      }
    }

    co_return;
  }

  Options options_;
  std::unique_ptr<IApiHandler> storage_;
  std::unique_ptr<ITokenAuthentication> auth_;
  std::unique_ptr<IAssetManager> console_;
};

std::unique_ptr<IApiServer> IApiServer::Build(Components components, Options options) {
  return std::make_unique<ApiServer>(std::move(components), std::move(options));
}

}  // namespace reduct::api
