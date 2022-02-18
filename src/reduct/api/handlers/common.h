// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLERS_COMMON_H
#define REDUCT_STORAGE_HANDLERS_COMMON_H

#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>
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
  explicit AsyncHttpReceiver(uWS::HttpResponse<SSL> *res) : data_(), finish_(false), res_(res) {
    res->onData([this](std::string_view data, bool last) mutable {
      LOG_TRACE("Received chuck {} kB", data.size() / 1024);
      data_ += data;
      finish_ = last;
    });
  }

  bool await_ready() const noexcept { return finish_; }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    if (finish_) {
      h.resume();
    } else {
      async::ILoop::loop().Defer([this, h] { await_suspend(h); });
    }
  }

  [[nodiscard]] std::string await_resume() noexcept { return std::move(data_); }

 private:
  std::string data_;
  bool finish_;
  uWS::HttpResponse<SSL> *res_;
};

template <bool SSL, typename Callback>
class BasicHandle {
 public:
  BasicHandle(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req)
      : http_resp_(res), http_req_(req), on_success_{}, url_(http_req_->getUrl()), method_(http_req_->getMethod()) {
    std::transform(method_.begin(), method_.end(), method_.begin(), [](auto &ch) { return std::toupper(ch); });
    http_resp_->onAborted([*this] { LOG_ERROR("{} {}: aborted", method_, url_); });
  }

  BasicHandle OnSuccess(std::function<std::string(typename Callback::Response)> func) {
    on_success_ = std::move(func);
    return std::move(*this);
  }

  core::Error Run(typename Callback::Result result) {
    auto [resp, err] = std::move(result);
    if (err) {
      SendError(err);
      return err;
    }

    LOG_DEBUG("{} {}: OK", method_, url_);
    http_resp_->end(on_success_ ? on_success_(std::move(resp)) : "");
    return {};
  }

  void SendError(core::Error err) const {
    LOG_ERROR("{} {}: {}", method_, url_, err.ToString());
    nlohmann::json data;
    http_resp_->writeStatus(std::to_string(err.code));

    data["detail"] = err.message;
    http_resp_->end(data.dump());
  }

 private:
  uWS::HttpResponse<SSL> *http_resp_;
  uWS::HttpRequest *http_req_;
  std::string url_;
  std::string method_;
  std::function<std::string(typename Callback::Response)> on_success_;
};
}  // namespace reduct::api
#endif  // REDUCT_STORAGE_HANDLERS_COMMON_H
