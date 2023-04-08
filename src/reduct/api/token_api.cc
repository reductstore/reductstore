// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/api/token_api.h"

namespace reduct::api {

using core::Error;
using core::Result;
using storage::IStorage;

Result<HttpRequestReceiver> TokenApi::CreateToken(rust_part::TokenRepository& repository, const IStorage* storage,
                                                  std::string_view name) {
  return ReceiveAndSendJson(
      [&repository, storage, name = std::string(name)](rust::String&& data) -> Result<rust::String> {
        auto check_bucket = [storage](const rust::Vec<rust::String>& bucket_list) -> Error {
          for (const auto& bucket : bucket_list) {
            if (storage->GetBucket(bucket.data()).error.code == Error::kNotFound) {
              return Error::UnprocessableEntity(fmt::format("Bucket '{}' doesn't exist", bucket.data()));
            }
          }
          return Error::kOk;
        };

        auto permissions = rust_part::new_token_permissions(false, {}, {});
        auto parsing_err = rust_part::json_to_token_permissions(data, *permissions);

        if (auto err = check_bucket(rust_part::token_permissions_read(*permissions))) {
          return {{}, err};
        }

        if (auto err = check_bucket(rust_part::token_permissions_write(*permissions))) {
          return {{}, err};
        }

        auto token = rust_part::new_token_create_response();

        auto err = rust_part::token_repo_create_token(repository, name.data(), *permissions, *token);
        if (err->status() != Error::kOk.code) {
          return Error(err->status(), err->message().data());
        }

        return rust_part::token_create_response_to_json(*token);
      });
}

Result<HttpRequestReceiver> TokenApi::ListTokens(rust_part::TokenRepository& repository) {
  rust::Vec<rust_part::Token> list;
  auto err = rust_part::token_repo_get_token_list(repository, list);
  if (err->status() != Error::kOk.code) {
    return Error(err->status(), err->message().data());
  }

  return SendJson(rust_part::token_list_to_json(list));
}

Result<HttpRequestReceiver> TokenApi::GetToken(rust_part::TokenRepository& repository, std::string_view name) {
  auto token = rust_part::new_token();
  auto err = rust_part::token_repo_get_token(repository, name.data(), *token);
  if (err->status() != Error::kOk.code) {
    return Error(err->status(), err->message().data());
  }

  return SendJson(rust_part::token_to_json(*token));
}

core::Result<HttpRequestReceiver> TokenApi::RemoveToken(rust_part::TokenRepository& repository, std::string_view name) {
  auto err = rust_part::token_repo_remove_token(repository, name.data());
  if (err->status() != Error::kOk.code) {
    return Error(err->status(), err->message().data());
  }
  return DefaultReceiver();
}
}  // namespace reduct::api
