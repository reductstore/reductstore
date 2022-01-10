// Copyright 2021-2022 Alexey Timin

#include "reduct/api/handlers/handle_bucket.h"

#include <nlohmann/json.hpp>

#include "reduct/api/handlers/common.h"
#include "reduct/async/sleep.h"
#include "reduct/config.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
VoidTask HandleCreateBucket(ICreateBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string name) {
  auto basic = BasicHandle<SSL, ICreateBucketCallback>(res, req);

  auto data = co_await AsyncHttpReceiver<SSL>(res);
  nlohmann::json json_data = nlohmann::json::parse(data, nullptr, false);
  if (!data.empty() && json_data.is_discarded()) {
    basic.SendError(Error{.code = 422, .message = "Failed parse JSON data"});
    co_return;
  }

  ICreateBucketCallback::Request app_request{
      .bucket_name = name,
      .bucket_settings = {
          .max_block_size =
              json_data.contains("max_block_size") ? json_data["max_block_size"].get<size_t>() : kDefaultMaxBlockSize,
          .quota_type = json_data.contains("quota_type") ? json_data["quota_type"].get<std::string_view>() : "NONE",
          .quota_size = json_data.contains("quota_size") ? json_data["quota_size"].get<size_t>() : 0,
      }};

  [[maybe_unused]] auto err = basic.Run(co_await callback->OnCreateBucket(app_request));
  co_return;
}

template VoidTask HandleCreateBucket<>(ICreateBucketCallback *handler, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string name);
template VoidTask HandleCreateBucket<>(ICreateBucketCallback *handler, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string name);

template <bool SSL>
VoidTask HandleGetBucket(IGetBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                         std::string name) {
  IGetBucketCallback::Request app_request{.bucket_name = name};
  [[maybe_unused]] auto err = BasicHandle<SSL, IGetBucketCallback>(res, req)
                                  .OnSuccess([](IGetBucketCallback::Response resp) {
                                    nlohmann::json data;
                                    data["max_block_size"] = resp.bucket_settings.max_block_size;
                                    data["quota_type"] = resp.bucket_settings.quota_type;
                                    data["quota_size"] = resp.bucket_settings.quota_size;
                                    return data.dump();
                                  })
                                  .Run(co_await callback->OnGetBucket(app_request));
  co_return;
}

template VoidTask HandleGetBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                    std::string name);
template VoidTask HandleGetBucket<>(IGetBucketCallback *handler, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                    std::string name);

template <bool SSL>
VoidTask HandleUpdateBucket(IUpdateBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                            std::string name) {
  auto basic = BasicHandle<SSL, IUpdateBucketCallback>(res, req);

  auto data = co_await AsyncHttpReceiver<SSL>(res);
  nlohmann::json json_data = nlohmann::json::parse(data, nullptr, false);
  if (json_data.is_discarded()) {
    basic.SendError(Error{.code = 422, .message = "Failed parse JSON data"});
    co_return;
  }

  IUpdateBucketCallback::Request app_request;
  try {
    const auto quota_type = json_data.at("quota_type").get<std::string>();
    app_request = {.bucket_name = name,
                   .new_settings = {.max_block_size = json_data.at("max_block_size"),
                                    .quota_type = quota_type,
                                    .quota_size = json_data.at("quota_size")}};
  } catch (const std::exception &err) {
    basic.SendError(Error{.code = 422, .message = fmt::format("Failed parse JSON data: {}", err.what())});
    co_return;
  }

  [[maybe_unused]] auto err = basic.Run(co_await callback->OnUpdateCallback(app_request));
  co_return;
}

template VoidTask HandleUpdateBucket<>(IUpdateBucketCallback *callback, uWS::HttpResponse<false> *res,
                                       uWS::HttpRequest *req, std::string name);
template VoidTask HandleUpdateBucket<>(IUpdateBucketCallback *callback, uWS::HttpResponse<true> *res,
                                       uWS::HttpRequest *req, std::string name);

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
