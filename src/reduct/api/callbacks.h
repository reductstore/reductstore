// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_CALLBACKS_H
#define REDUCT_STORAGE_CALLBACKS_H

#include <string>

#include "reduct/async/io.h"
#include "reduct/async/run.h"
#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/api/auth.pb.h"
#include "reduct/proto/api/bucket.pb.h"
#include "reduct/proto/api/entry.pb.h"
#include "reduct/proto/api/server.pb.h"

namespace reduct::api {

/**
 * Query callback
 */
class IQueryCallback {
 public:
  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view start_timestamp;
    std::string_view stop_timestamp;
    std::string_view ttl;
  };

  using Response = proto::api::QueryInfo;

  using Result = core::Result<Response>;
  virtual async::Run<Result> OnQuery(const Request& req) const = 0;
};

/**
 * Query callback
 */
class INextCallback {
 public:
  struct Response {
    async::IAsyncReader::SPtr reader;
    bool last;
  };

  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view id;
  };

  using Result = core::Result<Response>;
  virtual async::Run<Result> OnNextRecord(const Request& req) const = 0;
};

/**
 * API handler with all the request callbacks
 */
class IApiHandler : public IQueryCallback, public INextCallback {};
}  // namespace reduct::api
#endif  // REDUCT_STORAGE_CALLBACKS_H
