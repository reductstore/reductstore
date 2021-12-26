// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_UTILS_H
#define REDUCT_STORAGE_UTILS_H

#include <App.h>
#include <nlohmann/json.hpp>

#include "reduct/core/error.h"
#include "reduct/core/logger.h"

namespace reduct::api {

template <bool SSL>
inline void HandleError(uWS::HttpResponse<SSL> *res, const core::Error &error) {
  nlohmann::json data;
  res->writeStatus(std::to_string(error.code));

  data["detail"] = error.message;
  res->end(data.dump());
}
}  // namespace reduct::api

#endif  // REDUCT_STORAGE_UTILS_H
