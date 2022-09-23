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
      LOG_DEBUG("Received chuck {} kB", data.size() / 1024);
      error_ = callback(data, last);
      finish_ = last;
    });

    res->onAborted([this] {
      LOG_ERROR("Aborted write operation");
      error_ = core::Error{.code = 400};
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

template <bool SSL>
struct HttpContext {
  uWS::HttpResponse<SSL> *res;
  uWS::HttpRequest *req;
  bool running;
};

template <bool SSL, typename Callback>
class BasicApiHandler {
 public:
  explicit BasicApiHandler(HttpContext<SSL> ctx, std::string_view content_type = "application/json")
      : http_resp_(ctx.res),
        origin_(ctx.req->getHeader("origin")),
        authorization_(ctx.req->getHeader("authorization")),
        url_(ctx.req->getUrl()),
        method_(ctx.req->getMethod()),
        content_type_(content_type),
        running_(ctx.running),
        headers_{} {
    std::transform(method_.begin(), method_.end(), method_.begin(), [](auto &ch) { return std::toupper(ch); });
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

  void Send(
      typename Callback::Result &&result,
      std::function<std::string(typename Callback::Response)> on_success = [](auto) { return ""; }) const noexcept {
    auto [resp, err] = std::move(result);
    if (err) {
      SendError(err);
      return;
    }

    SendOk(on_success(std::move(resp)));
  }

  void SendOk(std::string_view content = "", bool no_headers = false) const noexcept {
    LOG_DEBUG("{} {}: OK", method_, url_);
    if (!no_headers) {
      PrepareHeaders(!content.empty(), content_type_);
    }
    http_resp_->end(std::move(content));
  }

  void SendError(core::Error err) const noexcept {
    if (err.code >= 500) {
      LOG_ERROR("{} {}: {}", method_, url_, err.ToString());
    } else {
      LOG_DEBUG("{} {}: {}", method_, url_, err.ToString());
    }

    http_resp_->writeStatus(std::to_string(err.code));
    PrepareHeaders(true, "application/json");
    http_resp_->end(fmt::format(R"({{"detail":"{}"}})", err.message));
  }

  template <class T>
  void AddHeader(std::string_view header, T value) noexcept {
    headers_[header] = fmt::format("{}", value);
  }

  void PrepareHeaders(bool has_content, std::string_view content_type = "") const {
    // Allow CORS
    if (!origin_.empty()) {
      http_resp_->writeHeader("access-control-allow-origin", origin_);
    }

    if (has_content && !content_type_.empty()) {
      http_resp_->writeHeader("content-type", content_type);
    }

    http_resp_->writeHeader("connection", running_ ? "keep-alive" : "close");
    http_resp_->writeHeader("server", "ReductStorage");

    for (auto &[key, val] : headers_) {
      http_resp_->writeHeader(key, val);
    }
  }

 private:
  uWS::HttpResponse<SSL> *http_resp_;
  std::string url_;
  std::string method_;
  std::string authorization_;
  std::string origin_;
  std::string_view content_type_;

  std::map<std::string_view, std::string> headers_;
  bool running_;
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_HANDLERS_COMMON_H
