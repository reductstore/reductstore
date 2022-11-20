// Copyright 2022 Alexey Timin

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

inline core::Result<Time> ParseTimestamp(std::string_view timestamp, std::string_view param_name = "ts") {
  auto ts = Time::clock::now();
  if (timestamp.empty()) {
    return {Time{}, Error::UnprocessableEntity(fmt::format("'{}' parameter can't be empty", param_name))};
  }
  try {
    auto ts_as_umber = std::stoll(timestamp.data());
    if (ts_as_umber < 0) {
      return {Time{}, Error::UnprocessableEntity(fmt::format("Failed to parse '{}' parameter: {} must be positive",
                                                             param_name, std::string{timestamp}))};
    }

    ts = Time() + std::chrono::microseconds(ts_as_umber);
    return {ts, Error::kOk};
  } catch (...) {
    return {Time{},
            Error::UnprocessableEntity(fmt::format("Failed to parse '{}' parameter: {} must unix times in microseconds",
                                                   param_name, std::string{timestamp}))};
  }
}

inline core::Result<uint64_t> ParseUInt(std::string_view timestamp, std::string_view param_name) {
  uint64_t val = 0;
  if (timestamp.empty()) {
    return {val, Error::UnprocessableEntity(fmt::format("'{}' parameter can't be empty", param_name))};
  }
  try {
    val = std::stoul(std::string{timestamp});
    return {val, Error::kOk};
  } catch (...) {
    return {val, Error::UnprocessableEntity(fmt::format("Failed to parse '{}' parameter: {} must be unsigned integer",
                                                        param_name, std::string{timestamp}))};
  }
}

inline core::Result<IEntry::SPtr> GetOrCreateEntry(IStorage* storage, const std::string& bucket_name,
                                                   const std::string& entry_name, bool must_exist = false) {
  auto [bucket_it, err] = storage->GetBucket(bucket_name);
  if (err) {
    return {{}, err};
  }

  auto bucket = bucket_it.lock();

  assert(bucket && "Failed to reach bucket");
  if (must_exist && !bucket->HasEntry(entry_name)) {
    return {{}, Error::NotFound(fmt::format("Entry '{}' is not found", entry_name))};
  }

  auto [entry, ref_error] = bucket->GetOrCreateEntry(entry_name);
  if (ref_error) {
    return {{}, ref_error};
  }

  auto entry_ptr = entry.lock();
  assert(bucket && "Failed to reach entry");

  if (!entry_ptr) {
    return {{}, Error::InternalError("Failed to resolve entry")};
  }

  return {entry_ptr, Error::kOk};
}

core::Result<HttpRequestReceiver> EntryApi::Write(storage::IStorage* storage, std::string_view bucket_name,
                                                  std::string_view entry_name, std::string_view timestamp,
                                                  std::string_view content_length) {
  auto [entry, create_err] = GetOrCreateEntry(storage, std::string(bucket_name), std::string(entry_name));
  if (create_err) {
    return DefaultReceiver(create_err);
  }

  auto [ts, parse_err] = ParseTimestamp(timestamp);
  if (parse_err) {
    return DefaultReceiver(parse_err);
  }

  int64_t size;
  try {
    size = std::stol(content_length.data());
    if (size < 0) {
      return DefaultReceiver(Error::ContentLengthRequired("Negative content-length"));
    }
  } catch (...) {
    return DefaultReceiver(Error::ContentLengthRequired("Bad or empty content-length"));
  }

  auto [writer, writer_err] = entry->BeginWrite(ts, size);
  if (writer_err) {
    return DefaultReceiver(writer_err);
  }

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
  auto [entry, create_err] = GetOrCreateEntry(storage, std::string(bucket_name), std::string(entry_name), true);
  if (create_err) {
    return DefaultReceiver(create_err);
  }

  bool last = true;
  async::IAsyncReader::SPtr reader;
  if (query_id.empty()) {
    Time ts;
    if (!timestamp.empty()) {
      auto [parsed_ts, parse_err] = ParseTimestamp(timestamp);
      if (parse_err) {
        return DefaultReceiver(parse_err);
      }

      ts = parsed_ts;
    } else {
      ts = Time() + std::chrono::microseconds(entry->GetInfo().latest_record());
    }

    auto [next, start_err] = entry->BeginRead(ts);
    if (start_err) {
      return DefaultReceiver(start_err);
    }
    reader = next;
  } else {
    auto [id, parse_err] = ParseUInt(query_id, "id");
    if (parse_err) {
      return DefaultReceiver(parse_err);
    }

    auto [next, start_err] = entry->Next(id);
    if (start_err) {
      return DefaultReceiver(start_err);
    } else if (start_err.code == 202) {
      return DefaultReceiver(start_err);
    }

    reader = next.reader;
    last = next.last;
  }

  assert(reader && "Failed to reach reader");
  return {
      [reader, last](std::string_view chunk, bool) -> Result<HttpResponse> {
        return Result<HttpResponse>{
            HttpResponse{
                .headers = {{"x-reduct-time", std::to_string(core::ToMicroseconds(reader->timestamp()))},
                            {"x-reduct-last", std::to_string(static_cast<int>(last))},
                            {"content-type", "application/octet-stream"}},
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

      Error::kOk,
  };
}

core::Result<HttpRequestReceiver> EntryApi::Query(storage::IStorage* storage, std::string_view bucket_name,
                                                  std::string_view entry_name, std::string_view start_timestamp,
                                                  std::string_view stop_timestamp, std::string_view ttl_interval) {
  auto [entry, err] = GetOrCreateEntry(storage, std::string(bucket_name), std::string(entry_name), true);
  if (err) {
    return DefaultReceiver(err);
  }

  std::optional<Time> start_ts;
  if (!start_timestamp.empty()) {
    auto [ts, parse_err] = ParseTimestamp(start_timestamp, "start_timestamp");
    if (parse_err) {
      return DefaultReceiver(parse_err);
    }
    start_ts = ts;
  }

  std::optional<Time> stop_ts;
  if (!stop_timestamp.empty()) {
    auto [ts, parse_err] = ParseTimestamp(stop_timestamp, "stop_timestamp");
    if (parse_err) {
      return DefaultReceiver(parse_err);
    }
    stop_ts = ts;
  }

  std::chrono::seconds ttl{5};
  if (!ttl_interval.empty()) {
    auto [val, parse_err] = ParseUInt(ttl_interval, "ttl");
    if (parse_err) {
      return DefaultReceiver(parse_err);
    }

    ttl = std::chrono::seconds(val);
  }

  auto [id, query_err] = entry->Query(start_ts, stop_ts, IQuery::Options{.ttl = ttl});
  if (query_err) {
    return DefaultReceiver(query_err);
  }

  QueryInfo info;
  info.set_id(id);

  return SendJson(Result<QueryInfo>{std::move(info), Error::kOk});
}

}  // namespace reduct::api
