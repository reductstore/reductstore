// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/handle_bucket.h"

#include <ranges>

#include "reduct/api/handlers/common.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
VoidTask HandleCreateBucket(ICreateBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string_view name) {
  ICreateBucketCallback::Request app_request{.name = std::string(name)};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, ICreateBucketCallback>(res, req).Run(co_await callback->OnCreateBucket(app_request));
  co_return;
}

template VoidTask HandleCreateBucket<>(ICreateBucketCallback *handler, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string_view name);
template VoidTask HandleCreateBucket<>(ICreateBucketCallback *handler, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string_view name);

template <bool SSL>
VoidTask HandleGetBucket(IGetBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                         std::string_view name) {
  IGetBucketCallback::Request app_request{.name = std::string(name)};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IGetBucketCallback>(res, req).Run(co_await callback->OnGetBucket(app_request));
  co_return;
}

template VoidTask HandleGetBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                    std::string_view name);
template VoidTask HandleGetBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                    std::string_view name);

template <bool SSL>
VoidTask HandleRemoveBucket(IRemoveBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string_view name) {
  IRemoveBucketCallback::Request app_request{.name = std::string(name)};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IRemoveBucketCallback>(res, req).Run(co_await callback->OnRemoveBucket(app_request));
  co_return;
}

template VoidTask HandleRemoveBucket<>(IRemoveBucketCallback *handler, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string_view name);
template VoidTask HandleRemoveBucket<>(IRemoveBucketCallback *handler, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string_view name);
}  // namespace reduct::api::handlers
