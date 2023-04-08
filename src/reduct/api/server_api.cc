// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/server_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using storage::IStorage;

Result<HttpRequestReceiver> ServerApi::Alive(const IStorage* storage) { return DefaultReceiver(); }

Result<HttpRequestReceiver> ServerApi::Info(const IStorage* storage) { return SendJson(storage->GetInfo()); }

Result<HttpRequestReceiver> ServerApi::List(const IStorage* storage) { return SendJson(storage->GetList()); }

core::Result<HttpRequestReceiver> ServerApi::Me(const rust_part::TokenRepository& token_repo,
                                                std::string_view auth_header) {
  auto token = rust_part::new_token();
  auto err = rust_part::token_repo_validate_token(token_repo, std::string(auth_header), *token);

  return SendJson(Result{rust_part::token_to_json(*token), Error::FromRust(*err)});
}

}  // namespace reduct::api
