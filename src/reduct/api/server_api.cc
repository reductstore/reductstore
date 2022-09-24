// Copyright 2022 Alexey Timin

#include "reduct/api/server_api.h"

namespace reduct::api {

core::Result<HttpResponse> ServerApi::Info(const storage::IStorage* storage) { return SendJson(storage->GetInfo()); }

core::Result<HttpResponse> ServerApi::List(const storage::IStorage* storage) { return SendJson(storage->GetList()); }

}  // namespace reduct::api
