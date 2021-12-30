// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/handle_bucket.h"

#include <ranges>

#include "reduct/api/handlers/common.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
VoidTask HandleCreateBucket(ICreateBucketCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string_view name) {
  ICreateBucketCallback::Request data{.name = std::string(name)};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, ICreateBucketCallback>(res, req).Run(co_await callback.OnCreateBucket(data));
  co_return;
}

template VoidTask HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string_view name);
template VoidTask HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string_view name);

}  // namespace reduct::api::handlers