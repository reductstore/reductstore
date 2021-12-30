// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/handle_info.h"

#include <nlohmann/json.hpp>

#include "reduct/api/handlers/common.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
async::VoidTask HandleInfo(const IInfoCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) {
  [[maybe_unused]] auto err = BasicHandle<SSL, IInfoCallback>(res, req)
                                  .OnSuccess([](IInfoCallback::Response app_resp) {
                                    nlohmann::json data;
                                    data["version"] = app_resp.version;
                                    data["bucket_number"] = app_resp.bucket_number;
                                    return data.dump();
                                  })
                                  .Run(co_await callback.OnInfo({}));
  co_return;
}

template VoidTask HandleInfo<>(const IInfoCallback &handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req);
template VoidTask HandleInfo<>(const IInfoCallback &handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req);

}  // namespace reduct::api::handlers
