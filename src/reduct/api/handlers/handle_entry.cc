// Copyright 2021-2022 Alexey Timin

#include "reduct/api/handlers/handle_entry.h"

#include <nlohmann/json.hpp>

#include "reduct/api/handlers/common.h"
#include "reduct/async/sleep.h"
#include "reduct/core/logger.h"

namespace reduct::api::handlers {

using async::VoidTask;
using core::Error;

template <bool SSL>
async::VoidTask HandleWriteEntry(IWriteEntryCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                 std::string bucket, std::string entry, std::string ts) {
  auto basic = BasicHandle<SSL, IWriteEntryCallback>(res, req);

  auto full_blob = co_await AsyncHttpReceiver<SSL>(res);
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

template <bool SSL = false>
async::VoidTask HandleListEntry(IListEntryCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                std::string bucket, std::string entry, std::string start_ts, std::string stop_ts) {
  IListEntryCallback::Request data{
      .bucket_name = bucket,
      .entry_name = entry,
      .start_timestamp = start_ts,
      .stop_timestamp = stop_ts,
  };

  [[maybe_unused]] auto err = BasicHandle<SSL, IListEntryCallback>(res, req)
                                  .OnSuccess([](IListEntryCallback::Response app_resp) {
                                    nlohmann::json data;
                                    for (const auto &rec : app_resp.records) {
                                      nlohmann::json record;
                                      record["ts"] = rec.timestamp;
                                      record["size"] = rec.size;
                                      data["records"].push_back(record);
                                    }
                                    return data.dump();
                                  })
                                  .Run(co_await callback->OnListEntry(data));
  co_return;
}

template VoidTask HandleListEntry(IListEntryCallback *callback, uWS::HttpResponse<false> *res, uWS::HttpRequest *req,
                                  std::string bucket, std::string entry, std::string start_ts, std::string stop_ts);

template VoidTask HandleListEntry(IListEntryCallback *callback, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                  std::string bucket, std::string entry, std::string start_ts, std::string stop_ts);
}  // namespace reduct::api::handlers
