// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/info.h"

#include <nlohmann/json.hpp>

#include "reduct/api/utils.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

template <bool SSL>
void HandleInfo(const IInfoCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) {
  IInfoCallback::Response info;
  if (auto err = callback.OnInfo(&info, {})) {
    LOG_ERROR("{}: {}", req->getUrl(), err.ToString());
    ResponseError<SSL>(res, err);
    return;
  }

  nlohmann::json data;
  data["version"] = info.version;
  res->end(data.dump());
}

template void HandleInfo<>(const IInfoCallback &handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req);
template void HandleInfo<>(const IInfoCallback &handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req);

}  // namespace reduct::api::handlers
