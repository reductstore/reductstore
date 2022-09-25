// Copyright 2022 Alexey Timin

#include "reduct/api/entry_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using core::Time;

using storage::IBucket;
using storage::IEntry;
using storage::IStorage;

inline core::Result<Time> ParseTimestamp(std::string_view timestamp, std::string_view param_name = "ts") {
  auto ts = Time::clock::now();
  if (timestamp.empty()) {
    return {Time{}, Error{.code = 422, .message = fmt::format("'{}' parameter can't be empty", param_name)}};
  }
  try {
    ts = Time{} + std::chrono::microseconds(std::stoull(std::string{timestamp}));
    return {ts, Error::kOk};
  } catch (...) {
    return {Time{}, Error{.code = 422,
                          .message = fmt::format("Failed to parse '{}' parameter: {} should unix times in microseconds",
                                                 param_name, std::string{timestamp})}};
  }
}

inline core::Result<IEntry::SPtr> GetOrCreateEntry(IStorage* storage, const std::string& bucket_name,
                                                   const std::string& entry_name, bool must_exist = false) {
  auto [bucket_it, err] = storage->GetBucket(bucket_name);
  if (err) {
    return {{}, err};
  }

  auto bucket = bucket_it.lock();
  if (must_exist && !bucket->HasEntry(entry_name)) {
    return {{}, {.code = 404, .message = fmt::format("Entry '{}' could not be found", entry_name)}};
  }

  auto [entry, ref_error] = bucket->GetOrCreateEntry(entry_name);
  if (ref_error) {
    return {{}, ref_error};
  }

  auto entry_ptr = entry.lock();
  if (!entry_ptr) {
    return {{}, {.code = 500, .message = "Failed to resolve entry"}};
  }

  return {entry_ptr, Error::kOk};
}

core::Result<HttpResponse> EntryApi::Write(storage::IStorage* storage, std::string_view bucket_name,
                                           std::string_view entry_name, std::string_view timestamp,
                                           std::string_view content_length) {
  auto [entry, err] = GetOrCreateEntry(storage, std::string(bucket_name), std::string(entry_name));
  if (err) {
    return {{}, err};
  }

  auto [ts, parse_err] = ParseTimestamp(timestamp);
  if (parse_err) {
    return {{}, parse_err};
  }

  int64_t size;
  try {
    size = std::stol(content_length.data());
    if (size < 0) {
      return {{}, {.code = 411, .message = "negative content-length"}};
    }
  } catch (...) {
    return {{}, {.code = 411, .message = "bad or empty content-length"}};
  }

  auto [writer, writer_err] = entry->BeginWrite(ts, size);
  if (!writer_err) {
    auto [bucket, _] = storage->GetBucket(std::string(bucket_name));
    auto quota_error = bucket.lock()->KeepQuota();
    if (quota_error) {
      LOG_WARNING("Didn't mange to keep quota: {}", quota_error.ToString());
    }
  }

  auto http_response = HttpResponse::Default();
  http_response.input_call = [writer](std::string_view chunk, bool last) { return writer->Write(chunk, last); };

  return { std::move(http_response), Error::kOk };
}

}  // namespace reduct::api
