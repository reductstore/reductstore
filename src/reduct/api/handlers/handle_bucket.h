// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLE_BUCKET_H
#define REDUCT_STORAGE_HANDLE_BUCKET_H

#include <uWebSockets/App.h>

#include "reduct/api/api_server.h"
#include "reduct/async/task.h"

namespace reduct::api::handlers {

/**
 * Handle HTTP request to creat a new bucket
 * @tparam SSL
 * @param callback Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 * @param name name of bucket
 */
template <bool SSL = false>
async::VoidTask HandleCreateBucket(ICreateBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                   std::string_view name);

/**
 * Handle HTTP request to get info about bucket
 * @tparam SSL
 * @tparam SSL
 * @param callback Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 * @param name name of bucket
 */
template <bool SSL = false>
async::VoidTask HandleGetBucket(IGetBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                std::string_view name);

/**
 * Handle HTTP request to remove a bucket
 * @tparam SSL
 * @tparam SSL
 * @param callback Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 * @param name name of bucket
 */
template <bool SSL = false>
async::VoidTask HandleRemoveBucket(IRemoveBucketCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                std::string_view name);

}  // namespace reduct::api::handlers

#endif  // REDUCT_STORAGE_HANDLE_BUCKET_H
