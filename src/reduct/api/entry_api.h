// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_ENTRY_API_H
#define REDUCT_STORAGE_ENTRY_API_H

#include "common.h"
#include "reduct/core/result.h"
#include "reduct/storage/storage.h"

namespace reduct::api {

class EntryApi {
 public:
  constexpr static std::string_view kLabelHeaderPrefix = "x-reduct-label-";
  /**
   * POST /b/:bucket_name/:entry
   */
  static core::Result<HttpRequestReceiver> Write(storage::IStorage* storage, std::string_view bucket_name,
                                                 std::string_view entry_name, std::string_view timestamp,
                                                 std::string_view content_length,
                                                 const storage::IEntry::LabelMap& labels = {});

  /**
   * GET /b/:bucket_name/:entry
   */
  static core::Result<HttpRequestReceiver> Read(storage::IStorage* storage, std::string_view bucket_name,
                                                std::string_view entry_name, std::string_view timestamp,
                                                std::string_view query_id);

  /**
   * GET /b/:bucket/:entry/query
   */
  static core::Result<HttpRequestReceiver> Query(storage::IStorage* storage, std::string_view bucket_name,
                                                 std::string_view entry_name, std::string_view start_timestamp,
                                                 std::string_view stop_timestamp, std::string_view ttl_interval);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_ENTRY_API_H
