// Copyright 2022 Alexey Timin

#include "reduct/api/server_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using storage::IStorage;

Result<HttpRequestReceiver> ServerApi::Alive(const IStorage* storage) {
  return DefaultReceiver(Error::kOk);
}

Result<HttpRequestReceiver> ServerApi::Info(const IStorage* storage) { return SendJson(storage->GetInfo()); }

Result<HttpRequestReceiver> ServerApi::List(const IStorage* storage) { return SendJson(storage->GetList()); }

}  // namespace reduct::api
