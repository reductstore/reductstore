// Copyright 2021 Alexey Timin

#include "reduct/api/handlers/bucket.h"

#include "reduct/api/utils.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

template <bool SSL>
void HandleCreateBucket(ICreateBucketCallback &callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                        std::string_view name) {
  ICreateBucketCallback::Request data{.name = std::string(name)};
  if (auto err = callback.OnCreateBucket(nullptr, data)) {
    LOG_ERROR("{}: {}", req->getUrl(), err.ToString());
    ResponseError<SSL>(res, err);
    return;
  }

  res->end({});
}

template void HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                   std::string_view name);
template void HandleCreateBucket<>(ICreateBucketCallback &handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                   std::string_view name);

}  // namespace reduct::api::handlers