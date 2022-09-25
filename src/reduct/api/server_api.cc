// Copyright 2022 Alexey Timin

#include "reduct/api/server_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using storage::IStorage;

Result<HttpResponse> ServerApi::Alive(const IStorage* storage) {
  return {
      HttpResponse::Default(),
      core::Error::kOk,
  };
}

Result<HttpResponse> ServerApi::Info(const IStorage* storage) { return SendJson(storage->GetInfo()); }

Result<HttpResponse> ServerApi::List(const IStorage* storage) { return SendJson(storage->GetList()); }

}  // namespace reduct::api
