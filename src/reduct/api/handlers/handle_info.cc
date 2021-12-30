// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/handle_info.h"

#include <nlohmann/json.hpp>

#include "reduct/api/handlers/common.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

using async::Task;

template <bool SSL>
void HandleInfo(const IInfoCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) {
  BasicHandle<SSL, IInfoCallback>(res, req)
      .OnSuccess([](IInfoCallback::Response app_resp) {
        nlohmann::json data;
        data["version"] = app_resp.version;
        data["bucket_number"] = app_resp.bucket_number;
        return data.dump();
      })
      .Run(callback.OnInfo({}));
}

template void HandleInfo<>(const IInfoCallback &handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req);
template void HandleInfo<>(const IInfoCallback &handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req);

}  // namespace reduct::api::handlers
