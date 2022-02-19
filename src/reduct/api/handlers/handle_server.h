// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLE_SERVER_H
#define REDUCT_STORAGE_HANDLE_SERVER_H

#include <App.h>

#include "reduct/api/api_server.h"
#include "reduct/async/task.h"

namespace reduct::api::handlers {

/**
 * @brief Handle Info request
 * Calls the corresponding callback and wrap data to JSON
 * @tparam SSL
 * @param callback Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 */
template <bool SSL = false>
async::VoidTask HandleInfo(const IInfoCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req);

/**
 * @brief Handle List request
 * Calls the corresponding callback and wrap data to JSON
 * @tparam SSL
 * @param callback Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 */
template <bool SSL = false>
async::VoidTask HandleList(const IListStorageCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req);


}  // namespace reduct::api::handlers

#endif  // REDUCT_STORAGE_HANDLE_SERVER_H
