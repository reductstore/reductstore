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
                                 std::string_view bucket, std::string_view entry, int64_t ts) {
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

  LOG_DEBUG("Received data");

  IWriteEntryCallback::Request data{
      .bucket_name = std::string(bucket),
      .entry_name = std::string(entry),
      .timestamp = ts,
      .blob = full_blob,
  };
  [[maybe_unused]] auto err =basic.Run(co_await callback->OnWriteEntry(data)); // TODO(Alexey Timin): std::move crushes
  co_return;
}

template VoidTask HandleWriteEntry<>(IWriteEntryCallback *callback, uWS::HttpResponse<false> *res,
                                     uWS::HttpRequest *req, std::string_view bucket, std::string_view entry,
                                     int64_t ts);
template VoidTask HandleWriteEntry<>(IWriteEntryCallback *callback, uWS::HttpResponse<true> *res, uWS::HttpRequest *req,
                                     std::string_view bucket, std::string_view entry, int64_t ts);

}  // namespace reduct::api::handlers
