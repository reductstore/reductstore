// Copyright 2021 Alexey Timin


#ifndef REDUCT_STORAGE_INFO_H
#define REDUCT_STORAGE_INFO_H

#include <App.h>

#include "reduct/api/api_server.h"

namespace reduct::api::handlers {

/**
 * @brief Handle Info request
 * Calls the corresponding callback and wrap data to JSON
 * @tparam SSL
 * @param handler Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 */
template <bool SSL = false>
void HandleInfo(const IApiHandler &handler, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req);

}  // namespace reduct::api::handlers

#endif  // REDUCT_STORAGE_INFO_H
