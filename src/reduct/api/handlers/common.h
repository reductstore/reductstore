// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLERS_COMMON_H
#define REDUCT_STORAGE_HANDLERS_COMMON_H

#include <nlohmann/json.hpp>
#include <uWebSockets/App.h>

#include "reduct/async/run.h"

namespace reduct::api {

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
      LOG_ERROR("{} {}: {}", method_, url_, err.ToString());
      nlohmann::json data;
      http_resp_->writeStatus(std::to_string(err.code));

      data["detail"] = err.message;
      http_resp_->end(data.dump());
      return err;
    }

    LOG_DEBUG("{} {}: OK", method_, url_);
    http_resp_->end(on_success_ ? on_success_(std::move(resp)) : "");
    return {};
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
