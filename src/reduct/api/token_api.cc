// Copyright 2022 Alexey Timin

#include "reduct/api/token_api.h"

namespace reduct::api {

using auth::ITokenRepository;
using core::Error;
using core::Result;
using proto::api::Token_Permissions;
using proto::api::TokenCreateResponse;
using proto::api::TokenRepo;
using storage::IStorage;

Result<HttpRequestReceiver> TokenApi::CreateToken(ITokenRepository* repository, const IStorage* storage,
                                                  std::string_view name) {
  return ReceiveAndSendJson<Token_Permissions, TokenCreateResponse>(
      [repository, storage, name = std::string(name)](Token_Permissions&& permissions) -> Result<TokenCreateResponse> {
        auto check_bucket = [storage](const auto& bucket_list) -> Error {
          for (const auto& bucket : bucket_list) {
            if (storage->GetBucket(bucket).error.code == Error::kNotFound) {
              return Error::UnprocessableEntity(fmt::format("Bucket '{}' doesn't exist", bucket));
            }
          }
          return Error::kOk;
        };

        if (auto err = check_bucket(permissions.read())) {
          return {{}, err};
        }

        if (auto err = check_bucket(permissions.write())) {
          return {{}, err};
        }

        return repository->CreateToken(name, permissions);
      });
}

Result<HttpRequestReceiver> TokenApi::ListTokens(ITokenRepository* repository) {
  auto [list, err] = repository->GetTokenList();
  if (err) {
    return DefaultReceiver(err);
  }

  proto::api::TokenRepo response;
  for (auto& token : list) {
    *response.add_tokens() = std::move(token);
  }

  return SendJson<TokenRepo>({std::move(response), Error::kOk});
}

Result<HttpRequestReceiver> TokenApi::GetToken(auth::ITokenRepository* repository, std::string_view name) {
  auto [token, err] = repository->FindByName(std::string(name));
  if (err) {
    return DefaultReceiver(err);
  }

  return SendJson<proto::api::Token>({std::move(token), Error::kOk});
}
core::Result<HttpRequestReceiver> TokenApi::RemoveToken(auth::ITokenRepository* repository, std::string_view name) {
  auto err = repository->RemoveToken(std::string(name));
  return DefaultReceiver(err);
}
}  // namespace reduct::api
