// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLERS_COMMON_H
#define REDUCT_STORAGE_HANDLERS_COMMON_H

#include <nlohmann/json.hpp>
#include <uWebSockets/App.h>

#include "reduct/async/run.h"

namespace reduct::api {

// template <bool SSL>
// async::Task<core::Error> BasicHandle(uWS::HttpResponse<SSL> *http_resp_, uWS::HttpRequest *http_req_,
//                                      std::function<core::Error()> &&handler,
//                                      std::function<void(uWS::HttpResponse<SSL> *http_resp_)> OnSuccess);

template <bool SSL, typename Callback>
class BasicHandle {
 public:
  BasicHandle(uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) : http_resp_(res), http_req_(req), on_success_{} {}

  //  BasicHandle Request(typename Callback::Request request) {
  //    app_req_ = std::move(request);
  //    return std::move(*this);
  //  }

  BasicHandle OnSuccess(std::function<std::string(typename Callback::Response)> func) {
    on_success_ = std::move(func);
    return std::move(*this);
  };

  async::Task<core::Error> Run(async::Run<typename Callback::Result> &&runner) {
    std::string url(http_req_->getUrl());
    std::string method(http_req_->getMethod());
    std::transform(method.begin(), method.end(), method.begin(), [](auto &ch) { return std::toupper(ch); });

    http_resp_->onAborted([method, url] { LOG_ERROR("{} {}: aborted", method, url); });
    BasicHandle this_ = std::move(*this);  // we move all context because the object is removed outside the coroutine

    auto result = runner.await_resume();
    if (result.second) {
      LOG_ERROR("{} {}: {}", method, url, result.second.ToString());
      nlohmann::json data;
      this_.http_resp_->writeStatus(std::to_string(result.second.code));

      data["detail"] = result.second.message;
      this_.http_resp_->end(data.dump());
      co_return result.second;
    }

    LOG_DEBUG("{} {}: OK", method, url);
    this_.http_resp_->end(this_.on_success_ ? this_.on_success_(std::move(result.first)) : "");
    co_return {};
  }

 private:
  uWS::HttpResponse<SSL> *http_resp_;
  uWS::HttpRequest *http_req_;
  //  typename Callback::Request app_req_;
  std::function<std::string(typename Callback::Response)> on_success_;
};
}  // namespace reduct::api
#endif  // REDUCT_STORAGE_HANDLERS_COMMON_H
