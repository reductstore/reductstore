// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_ENTRY_API_H
#define REDUCT_STORAGE_ENTRY_API_H

#include "api.h"
#include "reduct/core/result.h"
#include "reduct/storage/storage.h"

namespace reduct::api {

class EntryApi {
 public:
  /**
   * POST /b/:bucket_name/:entry
   */
  static core::Result<HttpResponse> Write(storage::IStorage* storage, std::string_view bucket_name,
                                         std::string_view entry_name, std::string_view timestamp,
                                         std::string_view content_length);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_ENTRY_API_H
