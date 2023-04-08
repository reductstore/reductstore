// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/server_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using storage::IStorage;

core::Result<std::string> ParseBearerToken(std::string_view authorization_header) {
  if (!authorization_header.starts_with("Bearer ")) {
    return {{}, Error::Unauthorized("No bearer token in request header")};
  }
  auto token_value = authorization_header.substr(7, authorization_header.size() - 7);
  return {std::string(token_value), Error::kOk};
}


Result<HttpRequestReceiver> ServerApi::Alive(const IStorage* storage) { return DefaultReceiver(); }

Result<HttpRequestReceiver> ServerApi::Info(const IStorage* storage) { return SendJson(storage->GetInfo()); }

Result<HttpRequestReceiver> ServerApi::List(const IStorage* storage) { return SendJson(storage->GetList()); }

core::Result<HttpRequestReceiver> ServerApi::Me(const rust_part::TokenRepository& token_repo,
                                                std::string_view auth_header) {
  auto value = ParseBearerToken(auth_header);
  auto token = rust_part::new_token();
  auto err = rust_part::token_repo_validate_token(token_repo, value.result.data(), *token);

  auto http_err = Error(err->status(), err->message().data());
  return SendJson(Result{rust_part::token_to_json(*token), http_err});
}

}  // namespace reduct::api
