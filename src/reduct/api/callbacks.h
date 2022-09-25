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

//---------------------
// Bucket API
//---------------------




/**
 * Remove bucket callback
 */
class IRemoveBucketCallback {
 public:
  struct Response {};
  struct Request {
    std::string_view bucket_name;
  };
  using Result = core::Result<Response>;
  virtual async::Run<Result> OnRemoveBucket(const Request& req) = 0;
};

/**
 * Change bucket settings callback
 */
class IUpdateBucketCallback {
 public:
  struct Response {};
  struct Request {
    std::string_view bucket_name;
    proto::api::BucketSettings new_settings;
  };

  using Result = core::Result<Response>;
  virtual async::Run<Result> OnUpdateCallback(const Request& req) = 0;
};
//---------------------
// Entry API
//---------------------

/**
 * Write a new record to the entry
 */
class IWriteEntryCallback {
 public:
  using Response = async::IAsyncWriter::SPtr;
  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view timestamp;
    std::string_view content_length;
  };

  using Result = core::Result<Response>;

  // NOTICE: Can't be an executor because we can't postpone receiving tasks in the loop
  virtual Result OnWriteEntry(const Request& req) noexcept = 0;
};

/**
 * Read a record by its timestamp
 */
class IReadEntryCallback {
 public:
  using Response = async::IAsyncReader::SPtr;
  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view timestamp;
    bool latest;
  };

  using Result = core::Result<Response>;
  virtual async::Run<Result> OnReadEntry(const Request& req) = 0;
};

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
class IApiHandler : public IRemoveBucketCallback,
                    public IUpdateBucketCallback,
                    public IWriteEntryCallback,
                    public IReadEntryCallback,
                    public IQueryCallback,
                    public INextCallback {};
}  // namespace reduct::api
#endif  // REDUCT_STORAGE_CALLBACKS_H
