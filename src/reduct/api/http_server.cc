// Copyright 2021-2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/http_server.h"

#include <App.h>
#include <fmt/format.h>

#include <filesystem>
#include <ranges>
#include <regex>

#include "reduct/api/bucket_api.h"
#include "reduct/api/console.h"
#include "reduct/api/entry_api.h"
#include "reduct/api/server_api.h"
#include "reduct/api/token_api.h"
#include "reduct/async/sleep.h"
#include "reduct/config.h"
#include "reduct/core/logger.h"

namespace reduct::api {

using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;

using proto::api::BucketSettings;

using asset::IAssetManager;
using async::Sleep;
using async::Task;
using async::VoidTask;
using auth::ITokenAuthorization;
using core::Error;
using core::Result;

using auth::Anonymous;
using auth::Authenticated;
using auth::FullAccess;
using auth::IAuthorizationPolicy;
using auth::ReadAccess;
using auth::WriteAccess;

namespace fs = std::filesystem;

class HttpServer : public IHttpServer {
 public:
  HttpServer(Components components, Options options)
      : storage_(std::move(components.storage)),
        auth_(std::move(components.auth)),
        token_repository_(std::move(components.token_repository)),
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
  struct AsyncHttpReceiver {
    using Callback = uWS::MoveOnlyFunction<core::Error(std::string_view, bool)>;
    explicit AsyncHttpReceiver(uWS::HttpResponse<SSL> *res, Callback callback) : finish_{}, error_{}, res_(res) {
      res->onData([this, callback = std::move(callback)](std::string_view data, bool last) mutable {
        LOG_DEBUG("Received chuck {} kB", data.size() / 1024);
        error_ = callback(data, last);
        finish_ = last;
      });

      res->onAborted([this] {
        LOG_ERROR("Aborted write operation");
        error_ = core::Error::BadRequest("Aborted write operation");
      });
    }

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) const noexcept {
      if (finish_ || error_) {
        h.resume();
      } else {
        async::ILoop::loop().Defer([this, h] { await_suspend(h); });
      }
    }

    [[nodiscard]] core::Error await_resume() noexcept { return error_; }

   private:
    bool finish_;
    core::Error error_;
    uWS::HttpResponse<SSL> *res_;
  };

  template <bool SSL>
  struct HttpContext {
    uWS::HttpResponse<SSL> *res;
    uWS::HttpRequest *req;
    bool running;
  };

  template <bool SSL>
  VoidTask RegisterEndpoint(const auth::IAuthorizationPolicy &policy, HttpContext<SSL> ctx,
                            std::function<Result<HttpRequestReceiver>()> &&callback) const {
    std::string method(ctx.req->getMethod());
    std::string url{ctx.req->getUrl()};
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
      ctx.res->writeHeader("server", fmt::format("ReductStore {}", kVersion));
    };

    auto SendError = [ctx, &method, &url, CommonHeaders](const core::Error &err) {
      if (err.code >= Error::kInternalError) {
        LOG_ERROR("{} {}: {}", method, url, err.ToString());
      } else {
        LOG_DEBUG("{} {}: {}", method, url, err.ToString());
      }

      ctx.res->writeStatus(std::to_string(err.code));
      CommonHeaders();
      ctx.res->writeHeader("content-type", "application/json");
      ctx.res->writeHeader("x-reduct-error", err.message);
      if (method == "HEAD") {
        ctx.res->end({});
      } else {
        ctx.res->end(fmt::format(R"({{"detail":"{}"}})", err.message));
      }
    };

    if (auto err = auth_->Check(authorization, *token_repository_, policy)) {
      SendError(err);
      co_return;
    }

    auto [receiver, err] = callback();
    if (err) {
      SendError(err);
      co_return;
    }

    HttpResponse response;
    err = co_await AsyncHttpReceiver<SSL>(ctx.res, [&response, &receiver](auto chunk, bool last) {
      auto [recv_resp, recv_err] = receiver(chunk, last);
      response = std::move(recv_resp);
      return recv_err;
    });

    if (err) {
      SendError(err);
      co_return;
    }

    ctx.res->writeStatus(std::to_string(err.code));  // If Ok but not 200
    CommonHeaders();
    for (auto &[key, val] : response.headers) {
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
      auto [chuck, read_err] = response.SendData();
      if (read_err) {
        SendError(read_err);
        co_return;
      }

      const auto offset = ctx.res->getWriteOffset();
      while (!aborted) {
        ready_to_continue = false;
        auto [ok, responded] =
            ctx.res->tryEnd(chuck.substr(ctx.res->getWriteOffset() - offset), response.content_length);
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

    LOG_DEBUG("Response for {} {} -- {}/{} kB", method, url, ctx.res->getWriteOffset() / 1024,
              response.content_length / 1024);

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

    const auto api_path = base_path + "api/v1/";
    // Server API
    app.head(api_path + "alive",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(Anonymous(), HttpContext<SSL>{res, req, running},
                                [this]() { return ServerApi::Alive(storage_.get()); });
             })
        .get(api_path + "info",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(Authenticated(), HttpContext<SSL>{res, req, running},
                                [this]() { return ServerApi::Info(storage_.get()); });
             })
        .get(api_path + "list",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(Authenticated(), HttpContext<SSL>{res, req, running},
                                [this]() { return ServerApi::List(storage_.get()); });
             })
        .get(api_path + "me",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(Authenticated(), HttpContext<SSL>{res, req, running}, [this, req]() {
                 return ServerApi::Me(token_repository_.get(), req->getHeader("authorization"));
               });
             })
        // Bucket API
        .post(api_path + "b/:bucket_name",
              [this, running](auto *res, auto *req) {
                RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running}, [this, req]() {
                  return BucketApi::CreateBucket(storage_.get(), req->getParameter(0));
                });
              })
        .get(api_path + "b/:bucket_name",
             [this, running](auto *res, auto *req) {
               std::string bucket_name(req->getParameter(0));
               RegisterEndpoint(Authenticated(), HttpContext<SSL>{res, req, running}, [this, req, &bucket_name]() {
                 return BucketApi::GetBucket(storage_.get(), bucket_name);
               });
             })
        .head(api_path + "b/:bucket_name",
              [this, running](auto *res, auto *req) {
                std::string bucket_name(req->getParameter(0));
                RegisterEndpoint(Authenticated(), HttpContext<SSL>{res, req, running}, [this, req, &bucket_name]() {
                  return BucketApi::HeadBucket(storage_.get(), bucket_name);
                });
              })
        .put(api_path + "b/:bucket_name",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running}, [this, req]() {
                 return BucketApi::UpdateBucket(storage_.get(), req->getParameter(0));
               });
             })
        .del(api_path + "b/:bucket_name",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running}, [this, req]() {
                 return BucketApi::RemoveBucket(storage_.get(), token_repository_.get(), req->getParameter(0));
               });
             })
        // Entry API
        .post(api_path + "b/:bucket_name/:entry_name",
              [this, running](auto *res, auto *req) {
                std::string bucket_name(req->getParameter(0));
                RegisterEndpoint(
                    WriteAccess(bucket_name), HttpContext<SSL>{res, req, running}, [this, req, &bucket_name]() {
                      storage::IEntry::LabelMap labels;
                      for (auto header = req->begin(); header != req->end(); ++header) {
                        const auto [key, value] = *header;
                        if (key.starts_with(EntryApi::kLabelHeaderPrefix)) {
                          labels[std::string(key.substr(EntryApi::kLabelHeaderPrefix.size()))] = std::string(value);
                        }
                      }
                      return EntryApi::Write(storage_.get(), bucket_name, req->getParameter(1), req->getQuery("ts"),
                                             req->getHeader("content-length"), req->getHeader("content-type"), labels);
                    });
              })
        .get(api_path + "b/:bucket_name/:entry_name",
             [this, running](auto *res, auto *req) {
               std::string bucket_name(req->getParameter(0));

               RegisterEndpoint(ReadAccess(bucket_name), HttpContext<SSL>{res, req, running},
                                [this, req, &bucket_name]() {
                                  return EntryApi::Read(storage_.get(), bucket_name, req->getParameter(1),
                                                        req->getQuery("ts"), req->getQuery("q"), true);
                                });
             })
        .head(api_path + "b/:bucket_name/:entry_name",
              [this, running](auto *res, auto *req) {
                std::string bucket_name(req->getParameter(0));
                std::string entry_name(req->getParameter(1));

                RegisterEndpoint(ReadAccess(bucket_name), HttpContext<SSL>{res, req, running},
                                 [this, req, &bucket_name, &entry_name]() {
                                   return EntryApi::Read(storage_.get(), bucket_name, entry_name, req->getQuery("ts"),
                                                         req->getQuery("q"), false);
                                 });
              })
        .get(api_path + "b/:bucket_name/:entry_name/q",
             [this, running](auto *res, auto *req) {
               std::string bucket_name(req->getParameter(0));
               RegisterEndpoint(ReadAccess(bucket_name), HttpContext<SSL>{res, req, running},
                                [this, req, bucket_name]() {
                                  EntryApi::QueryOptions options{.ttl = req->getQuery("ttl")};

                                  // parse include and exclude labels
                                  for (auto [key, value] : ParseQueryString(req->getQuery())) {
                                    if (key.starts_with("include-")) {
                                      options.include.emplace(key.substr(8), value);
                                    } else if (key.starts_with("exclude-")) {
                                      options.exclude.emplace(key.substr(8), value);
                                    }
                                  }

                                  return EntryApi::Query(storage_.get(), bucket_name, std::string(req->getParameter(1)),
                                                         std::string(req->getQuery("start")),
                                                         std::string(req->getQuery("stop")), options);
                                });
             })
        // Token API
        .get(api_path + "tokens",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running},
                                [this]() { return TokenApi::ListTokens(token_repository_.get()); });
             })
        .get(api_path + "tokens/:token_id",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running}, [this, req]() {
                 return TokenApi::GetToken(token_repository_.get(), req->getParameter(0));
               });
             })
        .post(api_path + "tokens/:token_id",
              [this, running](auto *res, auto *req) {
                RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running}, [this, req]() {
                  return TokenApi::CreateToken(token_repository_.get(), storage_.get(), req->getParameter(0));
                });
              })
        .del(api_path + "tokens/:token_id",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(FullAccess(), HttpContext<SSL>{res, req, running}, [this, req]() {
                 return TokenApi::RemoveToken(token_repository_.get(), req->getParameter(0));
               });
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
               RegisterEndpoint(Anonymous(), HttpContext<SSL>{res, req, running},
                                [&]() { return Console::UiRequest(console_.get(), base_path, path); });
             })
        .get(base_path + "ui",
             [this, base_path, running](auto *res, auto *req) {
               RegisterEndpoint(Anonymous(), HttpContext<SSL>{res, req, running},
                                [&]() { return Console::UiRequest(console_.get(), base_path, "index.html"); });
             })
        .any("/*",
             [this, running](auto *res, auto *req) {
               RegisterEndpoint(Anonymous(), HttpContext<SSL>{res, req, running},
                                []() -> Result<HttpRequestReceiver> { return Error::NotFound(); });
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

  Options options_;
  std::unique_ptr<storage::IStorage> storage_;
  std::unique_ptr<ITokenAuthorization> auth_;
  std::unique_ptr<auth::ITokenRepository> token_repository_;
  std::unique_ptr<IAssetManager> console_;
};

std::unique_ptr<IHttpServer> IHttpServer::Build(Components components, Options options) {
  return std::make_unique<HttpServer>(std::move(components), std::move(options));
}

}  // namespace reduct::api
