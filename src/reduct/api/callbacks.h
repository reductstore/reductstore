// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_CALLBACKS_H
#define REDUCT_STORAGE_CALLBACKS_H

#include <string>

#include "reduct/async/run.h"
#include "reduct/core/error.h"
#include "reduct/proto/api/bucket_info.pb.h"
#include "reduct/proto/api/bucket_settings.pb.h"
#include "reduct/proto/api/server_info.pb.h"
namespace reduct::api {

template <typename Response>
struct CallbackResult {
  Response response;
  core::Error error;

  operator const core::Error&() const { return error; }
};

//---------------------
// Server API
//---------------------

/**
 * Get info callback
 */
class IInfoCallback {
 public:
  struct Response {
    proto::api::ServerInfo info;
  };
  struct Request {};
  using Result = CallbackResult<Response>;
  virtual async::Run<Result> OnInfo(const Request& req) const = 0;
};

/**
 * Get list of buckets
 */
class IListStorageCallback {
 public:
  struct Response {
    proto::api::BucketInfoList buckets;
  };
  struct Request {};
  using Result = CallbackResult<Response>;
  virtual async::Run<Result> OnStorageList(const Request& req) const = 0;
};

//---------------------
// Bucket API
//---------------------

/**
 * Create bucket callback
 */
class ICreateBucketCallback {
 public:
  struct Response {};
  struct Request {
    std::string_view bucket_name;
    proto::api::BucketSettings bucket_settings;
  };
  using Result = CallbackResult<Response>;
  virtual async::Run<Result> OnCreateBucket(const Request& req) = 0;
};

/**
 * Get bucket callback
 */
class IGetBucketCallback {
 public:
  struct Response {
    proto::api::BucketSettings bucket_settings;
  };

  struct Request {
    std::string_view bucket_name;
  };
  using Result = CallbackResult<Response>;
  virtual async::Run<Result> OnGetBucket(const Request& req) const = 0;
};

/**
 * Remove bucket callback
 */
class IRemoveBucketCallback {
 public:
  struct Response {};
  struct Request {
    std::string_view bucket_name;
  };
  using Result = CallbackResult<Response>;
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

  using Result = CallbackResult<Response>;
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
  struct Response {};
  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view timestamp;
    std::string_view blob;
  };

  using Result = CallbackResult<Response>;
  virtual async::Run<Result> OnWriteEntry(const Request& req) = 0;
};

/**
 * Read a record by its timestamp
 */
class IReadEntryCallback {
 public:
  struct Response {
    std::string blob;
    std::string timestamp;
  };
  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view timestamp;
  };

  using Result = CallbackResult<Response>;
  virtual async::Run<Result> OnReadEntry(const Request& req) = 0;
};

class IListEntryCallback {
 public:
  struct RecordInfo {
    int64_t timestamp;
    size_t size;
  };

  struct Response {
    std::vector<RecordInfo> records;
  };

  struct Request {
    std::string_view bucket_name;
    std::string_view entry_name;
    std::string_view start_timestamp;
    std::string_view stop_timestamp;
  };

  using Result = CallbackResult<Response>;
  [[nodiscard]] virtual async::Run<Result> OnListEntry(const Request& req) const = 0;
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_CALLBACKS_H
