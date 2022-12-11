// Copyright 2022 Alexey Timin

#include "reduct/api/server_api.h"

#include "reduct/auth/token_auth.h"

namespace reduct::api {

using auth::ITokenRepository;
using auth::ParseBearerToken;
using core::Error;
using core::Result;
using storage::IStorage;

Result<HttpRequestReceiver> ServerApi::Alive(const IStorage* storage) { return DefaultReceiver(Error::kOk); }

Result<HttpRequestReceiver> ServerApi::Info(const IStorage* storage) { return SendJson(storage->GetInfo()); }

Result<HttpRequestReceiver> ServerApi::List(const IStorage* storage) { return SendJson(storage->GetList()); }

core::Result<HttpRequestReceiver> ServerApi::Me(const auth::ITokenRepository* token_repo,
                                                std::string_view auth_header) {
  auto [token_value, error] = ParseBearerToken(auth_header);
  if (error != Error::kOk) {
    return DefaultReceiver(error);
  }

  return SendJson(token_repo->FindByName(token_value));
}

}  // namespace reduct::api
