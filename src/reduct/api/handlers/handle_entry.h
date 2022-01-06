// Copyright 2021-2022 Alexey Timin

#ifndef REDUCT_STORAGE_HANDLE_ENTRY_H
#define REDUCT_STORAGE_HANDLE_ENTRY_H

#include <App.h>

#include "reduct/api/api_server.h"
#include "reduct/async/task.h"

namespace reduct::api::handlers {

/**
 * @brief Handle WriteEntry request
 * Calls the corresponding callback to write a blob
 * @tparam SSL
 * @param callback Handler class with all callbacks
 * @param res HTTP response
 * @param req HTTP request
 */
template <bool SSL = false>
async::VoidTask HandleWriteEntry(IWriteEntryCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                 std::string_view bucket, std::string_view entry, std::string_view ts);

/**
 * @brief Handle ReadEntry request
 * @tparam SSL
 * @param callback
 * @param res
 * @param req
 * @param bucket
 * @param entry
 * @param ts
 * @return
 */
template <bool SSL = false>
async::VoidTask HandleReadEntry(IReadEntryCallback *callback, uWS::HttpResponse<SSL> *res, uWS::HttpRequest *req,
                                 std::string_view bucket, std::string_view entry, std::string_view ts);

}  // namespace reduct::api::handlers

#endif  // REDUCT_STORAGE_HANDLE_INFO_H
