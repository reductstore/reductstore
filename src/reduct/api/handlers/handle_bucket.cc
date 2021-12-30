// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/handle_bucket.h"

#include <ranges>

#include "reduct/api/handlers/common.h"

namespace reduct::api::handlers {

using core::Error;

template <bool SSL>
void HandleCreateBucket(ICreateBucketCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                        std::string_view name) {
  ICreateBucketCallback::Request data{.name = std::string(name)};
  BasicHandle<SSL, ICreateBucketCallback>(res, req).Run(callback.OnCreateBucket(data));
}

template void HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                   std::string_view name);
template void HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                   std::string_view name);

}  // namespace reduct::api::handlers