// Copyright 2021 Alexey Timin

#include <App.h>

#include "reduct/async/awaiters.h"
#include "reduct/core/error.h"
#include "reduct/core/logger.h"

namespace reduct::api {

using async::Run;
using async::Task;
using core::Error;

//template <bool SSL>
//Task<Error> BasicHandle(uWS::HttpResponse<SSL> *http_resp_, uWS::HttpRequest *http_req_, std::function<Error()> &&handler,
//                        std::function<void(uWS::HttpResponse<SSL> *http_resp_)> OnSuccess) {
//  std::string url(http_req_->getUrl());
//  std::string method(http_req_->getMethod());
//  std::transform(method.begin(), method.end(), method.begin(), [](auto &ch) { return std::toupper(ch); });
//
//  http_resp_->onAborted([method, url] { LOG_ERROR("{} {}: aborted", method, url); });
//  if (auto err = co_await async::Run<Error>(std::move(handler))) {
//    LOG_ERROR("{} {}: {}", method, url, err.ToString());
//    nlohmann::json data;
//    http_resp_->writeStatus(std::to_string(err.code));
//
//    data["detail"] = err.message;
//    http_resp_->end(data.dump());
//    co_return err;
//  }
//
//  LOG_DEBUG("{} {}: OK", method, url);
//  OnSuccess(http_resp_);
//  co_return {};
//}
//
//template Task<Error> BasicHandle<>(uWS::HttpResponse<false> *http_resp_, uWS::HttpRequest *http_req_,
//                                   std::function<Error()> &&handler,
//                                   std::function<void(uWS::HttpResponse<false> *http_resp_)> OnSuccess);
//
//template Task<Error> BasicHandle<>(uWS::HttpResponse<true> *http_resp_, uWS::HttpRequest *http_req_,
//                                   std::function<Error()> &&handler,
//                                   std::function<void(uWS::HttpResponse<true> *http_resp_)> OnSuccess);

}  // namespace reduct::api