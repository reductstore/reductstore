// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/bucket.h"

#include <ranges>

#include "reduct/api/handlers/common.h"

namespace reduct::api::handlers {

using core::Error;

template <bool SSL>
void HandleCreateBucket(ICreateBucketCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                        std::string_view name) {
  ICreateBucketCallback::Request data{.name = std::string(name)};
  BasicHandle<SSL, ICreateBucketCallback>(res, req)
      .Request(std::move(data))
      .Run([&callback](auto app_resp, auto app_req) { return callback.OnCreateBucket(nullptr, app_req); });
}

template void HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                   std::string_view name);
template void HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                   std::string_view name);

}  // namespace reduct::api::handlers