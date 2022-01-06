// Copyright 2021-2022 Alexey Timin

#include <nlohmann/json.hpp>

#include "reduct/api/handlers/common.h"
#include "reduct/api/handlers/handle_info.h"
#include "reduct/async/sleep.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
async::VoidTask HandleWriteEntry(IWriteEntryCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                 std::string bucket, std::string entry, std::string ts) {
  auto basic = BasicHandle<SSL, IWriteEntryCallback>(res, req);

  std::string full_blob;
  bool finish = false;
  res->onData([&full_blob, &finish](std::string_view data, bool last) mutable {
    full_blob += std::string(data);
    finish = last;
  });

  while (!finish) {
    co_await async::Sleep(async::kTick);
  }

  IWriteEntryCallback::Request data{
      .bucket_name = bucket,
      .entry_name = entry,
      .timestamp = ts,
      .blob = full_blob,
  };
  [[maybe_unused]] auto err =
      basic.Run(co_await callback->OnWriteEntry(data));  // TODO(Alexey Timin): std::move crushes
  co_return;
}

template VoidTask HandleWriteEntry<>(IWriteEntryCallback *callback, uWS::HttpResponse<false> *res,
                                     uWS::HttpRequest *req, std::string bucket, std::string entry, std::string ts);
template VoidTask HandleWriteEntry<>(IWriteEntryCallback *callback, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                     std::string bucket, std::string entry, std::string ts);

template <bool SSL = false>
async::VoidTask HandleReadEntry(IReadEntryCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                std::string bucket, std::string entry, std::string ts) {
  IReadEntryCallback::Request data{
      .bucket_name = bucket,
      .entry_name = entry,
      .timestamp = ts,
  };

  [[maybe_unused]] auto err = BasicHandle<SSL, IReadEntryCallback>(res, req)
                                  .OnSuccess([](IReadEntryCallback::Response app_resp) { return app_resp.blob; })
                                  .Run(co_await callback->OnReadEntry(data));
  co_return;
}

template VoidTask HandleReadEntry<>(IReadEntryCallback *callback, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                    std::string bucket, std::string entry, std::string ts);
template VoidTask HandleReadEntry<>(IReadEntryCallback *callback, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                    std::string bucket, std::string entry, std::string ts);

}  // namespace reduct::api::handlers
