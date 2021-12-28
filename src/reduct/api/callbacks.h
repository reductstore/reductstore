// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_CALLBACKS_H
#define REDUCT_STORAGE_CALLBACKS_H

#include <string>

#include "reduct/core/error.h"

namespace reduct::api {

class IInfoCallback {
 public:

  struct Response {
    std::string version;
    size_t bucket_number;
  };

  struct Request {};

  virtual core::Error OnInfo(Response* res, const Request& req) const = 0;
};

class ICreateBucketCallback {
 public:
  struct Response {};

  struct Request {
    std::string name;
  };

  virtual core::Error OnCreateBucket(Response* res, const Request& req) = 0;
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_CALLBACKS_H
