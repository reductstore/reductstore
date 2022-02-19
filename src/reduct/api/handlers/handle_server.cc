// Copyright 2021-2022 Alexey Timin

#include "reduct/api/handlers/handle_server.h"

#include "reduct/api/handlers/common.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
VoidTask HandleInfo(const IInfoCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) {
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IInfoCallback>(res, req)
          .OnSuccess([](IInfoCallback::Response app_resp) { return PrintToJson(std::move(app_resp.info)); })
          .Run(co_await callback->OnInfo({}));
  co_return;
}

template VoidTask HandleInfo<>(const IInfoCallback *handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req);
template VoidTask HandleInfo<>(const IInfoCallback *handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req);

template <bool SSL>
VoidTask HandleList(const IListStorageCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req) {
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IListStorageCallback>(res, req)
          .OnSuccess([](IListStorageCallback::Response app_resp) { return PrintToJson(std::move(app_resp.buckets)); })
          .Run(co_await callback->OnStorageList({}));
  co_return;
}

template VoidTask HandleList<>(const IListStorageCallback *callback, uWS::HttpResponse<false> *res,
                               uWS::HttpRequest *req);
template VoidTask HandleList<>(const IListStorageCallback *callback, uWS::HttpResponse<true> *res,
                               uWS::HttpRequest *req);

}  // namespace reduct::api::handlers
