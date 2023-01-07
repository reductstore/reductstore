// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/entry_api.h"

#include "reduct/core/logger.h"
#include "reduct/proto/api/entry.pb.h"
#include "reduct/storage/query/quiery.h"

namespace reduct::api {

using core::Error;
using core::Result;
using core::Time;

using storage::IBucket;
using storage::IEntry;
using storage::IStorage;
using storage::query::IQuery;

using proto::api::QueryInfo;

inline Result<Time> ParseTimestamp(std::string_view timestamp, std::string_view param_name = "ts") {
  auto ts = Time::clock::now();
  if (timestamp.empty()) {
    return Error::UnprocessableEntity(fmt::format("'{}' parameter can't be empty", param_name));
  }

  try {
    auto ts_as_umber = std::stoll(timestamp.data());
    if (ts_as_umber < 0) {
      return Error::UnprocessableEntity(
          fmt::format("Failed to parse '{}' parameter: {} must be positive", param_name, std::string{timestamp}));
    }

    ts = Time() + std::chrono::microseconds(ts_as_umber);
    return {ts, Error::kOk};
  } catch (...) {
    return Error::UnprocessableEntity(fmt::format("Failed to parse '{}' parameter: {} must unix times in microseconds",
                                                  param_name, std::string{timestamp}));
  }
}

inline core::Result<uint64_t> ParseUInt(std::string_view timestamp, std::string_view param_name) {
  uint64_t val = 0;
  if (timestamp.empty()) {
    return Error::UnprocessableEntity(fmt::format("'{}' parameter can't be empty", param_name));
  }

  try {
    return std::stoul(std::string{timestamp});
  } catch (...) {
    return Error::UnprocessableEntity(
        fmt::format("Failed to parse '{}' parameter: {} must be unsigned integer", param_name, std::string{timestamp}));
  }
}

inline core::Result<IEntry::SPtr> GetOrCreateEntry(IStorage* storage, const std::string& bucket_name,
                                                   const std::string& entry_name, bool must_exist = false) {
  auto [bucket_it, err] = storage->GetBucket(bucket_name);
  RETURN_ERROR(err);

  auto bucket = bucket_it.lock();

  assert(bucket && "Failed to reach bucket");
  if (must_exist && !bucket->HasEntry(entry_name)) {
    return Error::NotFound(fmt::format("Entry '{}' is not found", entry_name));
  }

  IEntry::WPtr entry;
  RESULT_OR_RETURN_ERROR(entry, bucket->GetOrCreateEntry(entry_name));

  auto entry_ptr = entry.lock();
  assert(bucket && "Failed to reach entry");

  if (!entry_ptr) {
    return Error::InternalError("Failed to resolve entry");
  }

  return entry_ptr;
}

core::Result<HttpRequestReceiver> EntryApi::Write(storage::IStorage* storage, std::string_view bucket_name,
                                                  std::string_view entry_name, std::string_view timestamp,
                                                  std::string_view content_length, const IEntry::LabelMap& labels) {
  IEntry::SPtr entry;
  RESULT_OR_RETURN_ERROR(entry, GetOrCreateEntry(storage, std::string{bucket_name}, std::string{entry_name}));

  Time ts;
  RESULT_OR_RETURN_ERROR(ts, ParseTimestamp(timestamp));

  int64_t size;
  try {
    size = std::stol(content_length.data());
    if (size < 0) {
      return Error::ContentLengthRequired("Negative content-length");
    }
  } catch (...) {
    return Error::ContentLengthRequired("Bad or empty content-length");
  }

  auto [writer, writer_err] = entry->BeginWrite(ts, size, labels);
  RETURN_ERROR(writer_err);

  auto [bucket, _] = storage->GetBucket(std::string(bucket_name));
  auto quota_error = bucket.lock()->KeepQuota();
  if (quota_error) {
    LOG_WARNING("Didn't mange to keep quota: {}", quota_error.ToString());
  }

  return {
      [writer](std::string_view chunk, bool last) -> Result<HttpResponse> {
        auto resp = HttpResponse::Default();
        if (auto err = writer->Write(chunk, last)) {
          return Result<HttpResponse>{resp, err};
        }

        if (last) {
          return Result<HttpResponse>{resp, Error::kOk};
        }

        return Result<HttpResponse>{resp, Error::Continue()};
      },
      Error::kOk,
  };
}

Result<HttpRequestReceiver> EntryApi::Read(IStorage* storage, std::string_view bucket_name, std::string_view entry_name,
                                           std::string_view timestamp, std::string_view query_id) {
  IEntry::SPtr entry;
  RESULT_OR_RETURN_ERROR(entry, GetOrCreateEntry(storage, std::string{bucket_name}, std::string{entry_name}, true));

  bool last = true;
  async::IAsyncReader::SPtr reader;
  Error error = Error::kOk;
  if (query_id.empty()) {
    Time ts;
    if (!timestamp.empty()) {
      RESULT_OR_RETURN_ERROR(ts, ParseTimestamp(timestamp));
    } else {
      ts = Time() + std::chrono::microseconds(entry->GetInfo().latest_record());
    }

    RESULT_OR_RETURN_ERROR(reader, entry->BeginRead(ts));

  } else {
    size_t id;
    RESULT_OR_RETURN_ERROR(id, ParseUInt(query_id, "id"));

    auto [next, start_err] = entry->Next(id);
    RETURN_ERROR(start_err);
    if (start_err.code == Error::kNoContent) {
      return DefaultReceiver(std::move(start_err));
    }

    reader = next.reader;
    last = next.last;
    error = start_err;
  }

  assert(reader && "Failed to reach reader");
  return {
      [reader, last](std::string_view chunk, bool) -> Result<HttpResponse> {
        StringMap headers = {{"x-reduct-time", std::to_string(core::ToMicroseconds(reader->timestamp()))},
                             {"x-reduct-last", std::to_string(static_cast<int>(last))},
                             {"content-type", "application/octet-stream"}};

        for (const auto& [key, value] : reader->labels()) {
          headers.insert({fmt::format("{}{}", kLabelHeaderPrefix, key), value});
        }

        return Result<HttpResponse>{
            HttpResponse{
                .headers = std::move(headers),
                .content_length = reader->size(),
                .SendData =
                    [reader]() {
                      auto [chunk, err] = reader->Read();
                      return Result<std::string>{std::move(chunk.data), err};
                    },
            },
            Error::kOk,
        };
      },
      error,
  };
}

core::Result<HttpRequestReceiver> EntryApi::Query(storage::IStorage* storage, std::string_view bucket_name,
                                                  std::string_view entry_name, std::string_view start_timestamp,
                                                  std::string_view stop_timestamp, const QueryOptions& options) {
  auto [entry, err] = GetOrCreateEntry(storage, std::string(bucket_name), std::string(entry_name), true);
  RETURN_ERROR(err);

  std::optional<Time> start_ts;
  if (!start_timestamp.empty()) {
    RESULT_OR_RETURN_ERROR(start_ts, ParseTimestamp(start_timestamp, "start_timestamp"));
  }

  std::optional<Time> stop_ts;
  if (!stop_timestamp.empty()) {
    RESULT_OR_RETURN_ERROR(stop_ts, ParseTimestamp(stop_timestamp, "stop_timestamp"));
  }

  std::chrono::seconds ttl{5};
  if (!options.ttl.empty()) {
    auto [val, parse_err] = ParseUInt(options.ttl, "ttl");
    RETURN_ERROR(parse_err);
    ttl = std::chrono::seconds(val);
  }

  size_t id;
  RESULT_OR_RETURN_ERROR(id, entry->Query(start_ts, stop_ts,
                                          IQuery::Options{
                                              .ttl = ttl,
                                              .include = options.include,
                                              .exclude = options.exclude,
                                          }));

  QueryInfo info;
  info.set_id(id);

  return SendJson(Result<QueryInfo>{std::move(info), Error::kOk});
}

}  // namespace reduct::api
