// Copyright 2022 Alexey Timin

#include "reduct/api/http_server.h"

#include <App.h>
#include <fmt/format.h>

#include <filesystem>
#include <regex>

#include "reduct/api/bucket_api.h"
#include "reduct/api/common.h"
#include "reduct/api/server_api.h"
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

class HttpServer : public IHttpServer {
 public:
  HttpServer(Components components, Options options)
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
  VoidTask RegisterEndpoint(HttpContext<SSL> ctx, HttpResponseHandler &&handler) const {
    std::string method(ctx.req->getMethod());
    auto url = ctx.req->getUrl();
    auto authorization = ctx.req->getHeader("authorization");
    auto origin = ctx.req->getHeader("origin");

    std::transform(method.begin(), method.end(), method.begin(), [](auto ch) { return std::toupper(ch); });
    ctx.res->onAborted([&method, &url] { LOG_ERROR("{} {}: aborted", method, url); });

    auto CommonHeaders = [&ctx, &origin]() {
      // We must write headers before status
      if (!origin.empty()) {
        ctx.res->writeHeader("access-control-allow-origin", origin);
      }

      ctx.res->writeHeader("connection", ctx.running ? "keep-alive" : "close");
      ctx.res->writeHeader("server", "ReductStorage");
    };

    auto SendError = [ctx, &method, &url, CommonHeaders](core::Error err) {
      if (err.code >= 500) {
        LOG_ERROR("{} {}: {}", method, url, err.ToString());
      } else {
        LOG_DEBUG("{} {}: {}", method, url, err.ToString());
      }

      ctx.res->writeStatus(std::to_string(err.code));
      CommonHeaders();
      ctx.res->writeHeader("content-type", "application/json");
      ctx.res->end(fmt::format(R"({{"detail":"{}"}})", err.message));
    };

    if (auto err = auth_->Check(authorization)) {
      SendError(err);
      co_return;
    }

    auto result = handler();
    if (result.error) {
      SendError(result.error);
      co_return;
    }

    // Receive data
    auto [headers, content_length, input_call, output_call] = result.result;
    auto err = co_await AsyncHttpReceiver<SSL>(
        ctx.res, [&input_call](auto chunk, bool last) { return input_call(chunk, last); });
    if (err) {
      SendError(err);
      co_return;
    }

    CommonHeaders();
    for (auto &[key, val] : headers) {
      ctx.res->writeHeader(key, val);
    }

    // Send data
    bool ready_to_continue = false;
    ctx.res->onWritable([&ready_to_continue](auto _) {
      LOG_DEBUG("ready");
      ready_to_continue = true;
      return true;
    });

    bool aborted = false;
    ctx.res->onAborted([&aborted] {
      LOG_WARNING("aborted");
      aborted = true;
    });

    bool complete = false;
    while (!aborted && !complete) {
      co_await Sleep(async::kTick);  // switch context before start to read
      auto [chuck, read_err] = output_call();
      if (read_err) {
        SendError(read_err);
        co_return;
      }

      const auto offset = ctx.res->getWriteOffset();
      while (!aborted) {
        ready_to_continue = false;
        auto [ok, responded] = ctx.res->tryEnd(chuck.substr(ctx.res->getWriteOffset() - offset), content_length);
        if (ok) {
          complete = responded;
          break;
        } else {
          // Have to wait until onWritable sets flag
          while (!ready_to_continue && !aborted) {
            co_await Sleep(async::kTick);
          }

          continue;
        }
      }
    }

    LOG_DEBUG("Response for {} {} -- {}/{} kB", method, url, ctx.res->getWriteOffset() / 1024, content_length / 1024);

    co_return;
  }

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
    app.head(base_path + "alive",
             [this, running](auto *res, auto *req) {
               Alive(HttpContext<SSL>{res, req, running});
             })
        .get(base_path + "info",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(HttpContext<SSL>{res, req, running},
                                [this]() { return ServerApi::Info(storage_.get()); });
             })
        .get(base_path + "list",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(HttpContext<SSL>{res, req, running},
                                [this]() { return ServerApi::List(storage_.get()); });
             })
        // Bucket API
        .post(base_path + "b/:bucket_name",
              [this, running](auto *res, auto *req) {
                RegisterEndpoint(HttpContext<SSL>{res, req, running}, [this, req]() {
                  return BucketApi::CreateBucket(storage_.get(), req->getParameter(0));
                });
              })
        .get(base_path + "b/:bucket_name",
             [this, running](auto *res, auto *req) {
               GetBucket(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)));
             })
        .head(base_path + "b/:bucket_name",
              [this, running](auto *res, auto *req) {
                HeadBucket(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)));
              })
        .put(base_path + "b/:bucket_name",
             [this, running](auto *res, auto *req) {
               UpdateBucket(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)));
             })
        .del(base_path + "b/:bucket_name",
             [this, running](auto *res, auto *req) {
               RemoveBucket(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)));
             })
        // Entry API
        .post(base_path + "b/:bucket_name/:entry_name",
              [this, running](auto *res, auto *req) {
                WriteEntry(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)),
                           std::string(req->getParameter(1)), std::string(req->getQuery("ts")));
              })
        .get(base_path + "b/:bucket_name/:entry_name",
             [this, running](auto *res, auto *req) {
               ReadEntry(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)),
                         std::string(req->getParameter(1)), std::string(req->getQuery("ts")),
                         std::string(req->getQuery("q")));
             })
        .get(base_path + "b/:bucket_name/:entry_name/q",
             [this, running](auto *res, auto *req) {
               QueryEntry(HttpContext<SSL>{res, req, running}, std::string(req->getParameter(0)),
                          std::string(req->getParameter(1)), std::string(req->getQuery("start")),
                          std::string(req->getQuery("stop")), std::string(req->getQuery("ttl")));
             })
        .get(base_path,
             [base_path](auto *res, auto *req) {
               res->writeStatus("301");
               res->writeHeader("location", base_path + "ui/");
               res->end({});
             })
        .get(base_path + "ui/*",
             [this, base_path, running](auto *res, auto *req) {
               std::string path(req->getUrl());
               path = path.substr(base_path.size() + 3, path.size());

               if (path.empty()) {
                 path = "index.html";
               }

               UiRequest(HttpContext<SSL>{res, req, running}, base_path, path);
             })
        .get(base_path + "ui",
             [this, base_path, running](auto *res, auto *req) {
               UiRequest(HttpContext<SSL>{res, req, running}, base_path, "index.html");
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
                      us_listen_socket_close(SSL, sock);
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
  VoidTask Alive(HttpContext<SSL> ctx) const {
    class IAliveCallback {
     public:
      struct Response {};
      struct Request {};
      using Result = core::Result<Response>;
    };

    auto handler = BasicApiHandler<SSL, IAliveCallback>(ctx, "");
    typename IAliveCallback::Result result{};
    handler.Send(std::move(result));
    co_return;
  }

  /**
   * GET /b/:name
   */
  template <bool SSL>
  VoidTask GetBucket(HttpContext<SSL> ctx, std::string name) const {
    auto handler = BasicApiHandler<SSL, IGetBucketCallback>(ctx);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IGetBucketCallback::Request app_request{.bucket_name = name};
    handler.Send(co_await storage_->OnGetBucket(app_request),
                 [](const IGetBucketCallback::Response &resp) { return PrintToJson(resp); });
    co_return;
  }

  /**
   * HEAD /b/:name
   */
  template <bool SSL>
  VoidTask HeadBucket(HttpContext<SSL> ctx, std::string name) const {
    auto handler = BasicApiHandler<SSL, IGetBucketCallback>(ctx);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IGetBucketCallback::Request app_request{.bucket_name = name};
    handler.Send(co_await storage_->OnGetBucket(app_request));
    co_return;
  }

  /**
   * PUT /b/:name
   */
  template <bool SSL>
  VoidTask UpdateBucket(HttpContext<SSL> ctx, std::string name) const {
    auto handler = BasicApiHandler<SSL, IUpdateBucketCallback>(ctx);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    std::string data;
    auto err = co_await AsyncHttpReceiver<SSL>(ctx.res, [&data](auto chuck, bool _) {
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
    handler.Send(co_await storage_->OnUpdateCallback(app_request));
    co_return;
  }

  /**
   * DELETE /b/:name
   */
  template <bool SSL>
  VoidTask RemoveBucket(HttpContext<SSL> ctx, std::string name) const {
    auto handler = BasicApiHandler<SSL, IRemoveBucketCallback>(ctx);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IRemoveBucketCallback::Request app_request{.bucket_name = name};
    handler.Send(co_await storage_->OnRemoveBucket(app_request));
    co_return;
  }

  // Entry API
  /**
   * POST /b/:bucket/:entry
   */
  template <bool SSL>
  async::VoidTask WriteEntry(HttpContext<SSL> ctx, std::string bucket, std::string entry, std::string ts) const {
    auto handler = BasicApiHandler<SSL, IWriteEntryCallback>(ctx);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }
    IWriteEntryCallback::Request app_req{
        .bucket_name = bucket,
        .entry_name = entry,
        .timestamp = ts,
        .content_length = ctx.req->getHeader("content-length"),
    };

    auto [async_writer, err] = storage_->OnWriteEntry(app_req);
    if (err) {
      handler.SendError(err);
      co_return;
    }

    err = co_await AsyncHttpReceiver<SSL>(
        ctx.res, [async_writer](auto chunk, bool last) { return async_writer->Write(chunk, last); });
    if (err) {
      handler.SendError(err);
      co_return;
    }

    handler.SendOk();
    co_return;
  }

  /**
   * GET /b/:bucket/:entry
   */
  template <bool SSL = false>
  async::VoidTask ReadEntry(HttpContext<SSL> ctx, std::string bucket, std::string entry, std::string ts,
                            std::string query_id) const {
    auto handler = BasicApiHandler<SSL, IReadEntryCallback>(ctx, "application/octet-stream");
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    Error err;
    async::IAsyncReader::SPtr reader;
    bool last = true;
    if (query_id.empty()) {
      IReadEntryCallback::Request data{
          .bucket_name = bucket, .entry_name = entry, .timestamp = ts, .latest = ts.empty()};
      auto ret = co_await storage_->OnReadEntry(data);
      reader = ret.result;
      err = ret.error;
    } else {
      INextCallback::Request data{.bucket_name = bucket, .entry_name = entry, .id = query_id};
      auto ret = co_await storage_->OnNextRecord(data);
      reader = ret.result.reader;
      last = ret.result.last;
      err = ret.error;
    }

    if (err || !reader) {
      handler.SendError(err);
      co_return;
    }
    bool ready_to_continue = false;
    ctx.res->onWritable([&ready_to_continue](auto _) {
      LOG_DEBUG("ready");
      ready_to_continue = true;
      return true;
    });

    bool aborted = false;
    ctx.res->onAborted([&aborted] {
      LOG_WARNING("aborted");
      aborted = true;
    });

    handler.AddHeader("x-reduct-time", core::ToMicroseconds(reader->timestamp()));
    handler.AddHeader("x-reduct-last", static_cast<int>(last));
    handler.PrepareHeaders(true, "application/octet-stream");

    bool complete = false;
    while (!aborted && !complete) {
      co_await Sleep(async::kTick);  // switch context before start to read
      auto [chuck, read_err] = reader->Read();
      if (read_err) {
        handler.SendError(err);
        co_return;
      }

      const auto offset = ctx.res->getWriteOffset();
      while (!aborted) {
        ready_to_continue = false;
        auto [ok, responded] = ctx.res->tryEnd(chuck.data.substr(ctx.res->getWriteOffset() - offset), reader->size());
        if (ok) {
          complete = responded;
          break;
        } else {
          // Have to wait until onWritable sets flag
          LOG_DEBUG("Failed to send data: {} {}/{} kB", ts, ctx.res->getWriteOffset() / 1024, reader->size() / 1024);
          while (!ready_to_continue && !aborted) {
            co_await Sleep(async::kTick);
          }

          continue;
        }
      }
    }

    LOG_DEBUG("Sent {} {}/{} kB", ts, ctx.res->getWriteOffset() / 1024, reader->size() / 1024);

    handler.SendOk("", true);
    co_return;
  }

  /**
   * GET /b/:bucket/:entry/query
   */
  template <bool SSL = false>
  async::VoidTask QueryEntry(HttpContext<SSL> ctx, std::string bucket, std::string entry, std::string start_ts,
                             std::string stop_ts, std::string ttl) const {
    auto handler = BasicApiHandler<SSL, IQueryCallback>(ctx);
    if (handler.CheckAuth(auth_.get()) != Error::kOk) {
      co_return;
    }

    IQueryCallback::Request data{
        .bucket_name = bucket,
        .entry_name = entry,
        .start_timestamp = start_ts,
        .stop_timestamp = stop_ts,
        .ttl = ttl,
    };

    handler.Send(co_await storage_->OnQuery(data),
                 [](const IQueryCallback::Response &app_resp) { return PrintToJson(app_resp); });
    co_return;
  }

  template <bool SSL = false>
  async::VoidTask UiRequest(HttpContext<SSL> ctx, std::string_view base_path, std::string path) const {
    struct Callback {
      struct [[maybe_unused]] Request {};  // NOLINT
      using Response = std::string;
      using Result = core::Result<Response>;
    };

    auto handler = BasicApiHandler<SSL, Callback>(ctx, "");

    auto replace_base_path = [&base_path](typename Callback::Response resp) {
      // substitute RS_API_BASE_PATH to make web console work
      resp = std::regex_replace(resp, std::regex("/ui/"), fmt::format("{}ui/", base_path));
      return resp;
    };

    auto ret = console_->Read(path);
    switch (ret.error.code) {
      case 200: {
        auto content = std::regex_replace(ret.result, std::regex("/ui/"), fmt::format("{}ui/", base_path));
        ctx.res->end(std::move(content));
        handler.Send(std::move(ret), replace_base_path);
        break;
      }
      case 404: {
        // It's React.js paths
        ret = console_->Read("index.html");
        handler.Send(std::move(ret), replace_base_path);
        break;
      }
      default: {
        handler.SendError(ret.error);
      }
    }

    co_return;
  }

  Options options_;
  std::unique_ptr<storage::IStorage> storage_;
  std::unique_ptr<ITokenAuthentication> auth_;
  std::unique_ptr<IAssetManager> console_;
};

std::unique_ptr<IHttpServer> IHttpServer::Build(Components components, Options options) {
  return std::make_unique<HttpServer>(std::move(components), std::move(options));
}

}  // namespace reduct::api
