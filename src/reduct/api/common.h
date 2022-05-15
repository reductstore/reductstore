// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLERS_COMMON_H
#define REDUCT_STORAGE_HANDLERS_COMMON_H

#include <google/protobuf/util/json_util.h>
#include <uWebSockets/App.h>

#include "reduct/async/loop.h"
#include "reduct/async/run.h"
#include "reduct/core/error.h"
#include "reduct/core/logger.h"

namespace reduct::api {

template <class T>
std::string PrintToJson(T &&msg) {
  using google::protobuf::util::JsonPrintOptions;
  using google::protobuf::util::MessageToJsonString;

  std::string data;
  JsonPrintOptions options;
  options.preserve_proto_field_names = true;
  options.always_print_primitive_fields = true;
  MessageToJsonString(msg, &data, options);
  return data;
}

template <bool SSL>
struct AsyncHttpReceiver {
  using Callback = uWS::MoveOnlyFunction<core::Error(std::string_view, bool)>;
  explicit AsyncHttpReceiver(uWS::HttpResponse<SSL> *res, Callback callback) : finish_{}, error_{}, res_(res) {
    res->onData([this, callback = std::move(callback)](std::string_view data, bool last) mutable {
      LOG_TRACE("Received chuck {} kB", data.size() / 1024);
      error_ = callback(data, last);
      finish_ = last;
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
class WhenWritable {
 public:
  explicit WhenWritable(uWS::HttpResponse<SSL> *res) : ready_{}, max_size_{} {
    res->onWritable([this](auto max) -> bool {
      ready_ = true;
      max_size_ = max;
      return true;
    });
  }

  [[nodiscard]] bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    if (ready_) {
      h.resume();
    } else {
      async::ILoop::loop().Defer([this, h] { await_suspend(h); });
    }
  }

  [[nodiscard]] size_t await_resume() noexcept { return max_size_; }

 private:
  bool ready_;
  size_t max_size_;
};

template <bool SSL, typename Callback>
class BasicApiHandler {
 public:
  BasicApiHandler(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req)
      : http_resp_(res),
        authorization_(req->getHeader("authorization")),
        url_(req->getUrl()),
        method_(req->getMethod()) {
    std::transform(method_.begin(), method_.end(), method_.begin(), [](auto &ch) { return std::toupper(ch); });

    // Allow CORS
    auto origin = req->getHeader("origin");
    if (!origin.empty()) {
      res->writeHeader("access-control-allow-origin", origin);
    }

    http_resp_->onAborted([*this] { LOG_ERROR("{} {}: aborted", method_, url_); });
  }

  core::Error CheckAuth(auth::ITokenAuthentication *auth) const noexcept {
    auto err = core::Error::kOk;
    if (auth) {
      err = auth->Check(authorization_);
    }

    if (err) {
      SendError(err);
    }

    return err;
  }

  void Run(
      typename Callback::Result &&result,
      std::function<std::string(typename Callback::Response)> on_success = [](auto) { return ""; }) const noexcept {
    auto [resp, err] = std::move(result);
    if (err) {
      SendError(err);
      return;
    }

    LOG_DEBUG("{} {}: OK", method_, url_);
    http_resp_->end(on_success(std::move(resp)));
  }

  void SendError(core::Error err) const noexcept {
    if (err.code >= 500) {
      LOG_ERROR("{} {}: {}", method_, url_, err.ToString());
    }
    http_resp_->writeStatus(std::to_string(err.code));
    http_resp_->end(fmt::format(R"({{"detail":"{}"}})", err.message));
  }

 private:
  uWS::HttpResponse<SSL> *http_resp_;
  std::string url_;
  std::string method_;
  std::string authorization_;
};
}  // namespace reduct::api
#endif  // REDUCT_STORAGE_HANDLERS_COMMON_H
