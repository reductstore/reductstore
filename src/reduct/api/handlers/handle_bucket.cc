// Copyright 2021-2022 Alexey Timin

#include "reduct/api/handlers/handle_bucket.h"

#include <google/protobuf/util/json_util.h>

#include "reduct/api/handlers/common.h"
#include "reduct/async/sleep.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;
using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;

using proto::api::BucketSettings;
/**
 * POST
 */
template <bool SSL>
VoidTask HandleCreateBucket(ICreateBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string name) {
  auto basic = BasicHandle<SSL, ICreateBucketCallback>(res, req);

  auto data = co_await AsyncHttpReceiver<SSL>(res);
  BucketSettings settings;
  if (!data.empty()) {
    auto status = JsonStringToMessage(data, &settings);
    if (!status.ok()) {
      basic.SendError(
          Error{.code = 422, .message = fmt::format("Failed parse JSON data: {}", status.message().ToString())});
      co_return;
    }
  }

  ICreateBucketCallback::Request app_request{
      .bucket_name = name,
      .bucket_settings = std::move(settings),
  };

  [[maybe_unused]] auto err = basic.Run(co_await callback->OnCreateBucket(app_request));
  co_return;
}

template VoidTask HandleCreateBucket<>(ICreateBucketCallback *handler, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string name);
template VoidTask HandleCreateBucket<>(ICreateBucketCallback *handler, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string name);
/**
 * GET
 */
template <bool SSL>
VoidTask HandleGetBucket(IGetBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                         std::string name) {
  IGetBucketCallback::Request app_request{.bucket_name = name};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IGetBucketCallback>(res, req)
          .OnSuccess([](IGetBucketCallback::Response resp) { return PrintToJson(resp.bucket_settings); })
          .Run(co_await callback->OnGetBucket(app_request));
  co_return;
}

template VoidTask HandleGetBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                    std::string name);
template VoidTask HandleGetBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                    std::string name);

/**
 * HEAD
 */
template <bool SSL>
VoidTask HandleHeadBucket(IGetBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                          std::string name) {
  IGetBucketCallback::Request app_request{.bucket_name = name};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IGetBucketCallback>(res, req).Run(co_await callback->OnGetBucket(app_request));
  co_return;
}

template VoidTask HandleHeadBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                     std::string name);
template VoidTask HandleHeadBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                     std::string name);

/**
 * PUT
 */
template <bool SSL>
VoidTask HandleUpdateBucket(IUpdateBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string name) {
  auto basic = BasicHandle<SSL, IUpdateBucketCallback>(res, req);

  auto data = co_await AsyncHttpReceiver<SSL>(res);
  BucketSettings settings;
  auto status = JsonStringToMessage(data, &settings);
  if (!status.ok()) {
    basic.SendError(Error{.code = 422, .message = "Failed parse JSON data"});
    co_return;
  }

  IUpdateBucketCallback::Request app_request{
      .bucket_name = name,
      .new_settings = std::move(settings),
  };
  [[maybe_unused]] auto err = basic.Run(co_await callback->OnUpdateCallback(app_request));
  co_return;
}

template VoidTask HandleUpdateBucket<>(IUpdateBucketCallback *callback, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string name);
template VoidTask HandleUpdateBucket<>(IUpdateBucketCallback *callback, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string name);

/**
 * DELETE
 */
template <bool SSL>
VoidTask HandleRemoveBucket(IRemoveBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string name) {
  IRemoveBucketCallback::Request app_request{.bucket_name = name};
  [[maybe_unused]] auto err =
      BasicHandle<SSL, IRemoveBucketCallback>(res, req).Run(co_await callback->OnRemoveBucket(app_request));
  co_return;
}

template VoidTask HandleRemoveBucket<>(IRemoveBucketCallback *handler, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string name);
template VoidTask HandleRemoveBucket<>(IRemoveBucketCallback *handler, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string name);
}  // namespace reduct::api::handlers
