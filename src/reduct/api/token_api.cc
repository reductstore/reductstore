// Copyright 2022 Alexey Timin

#include "token_api.h"

namespace reduct::api {

using auth::ITokenRepository;
using core::Error;
using core::Result;
using proto::api::Token_Permissions;
using proto::api::TokenCreateResponse;
using proto::api::TokenRepo;

Result<HttpRequestReceiver> TokenApi::CreateToken(ITokenRepository* repository, std::string_view name) {
  return ReceiveAndSendJson<Token_Permissions, TokenCreateResponse>(
      [repository, name = std::string(name)](Token_Permissions&& permissions) -> Result<TokenCreateResponse> {
        auto [token, err] = repository->Create(name, permissions);

        TokenCreateResponse resp;
        resp.set_value(token);
        return Result<TokenCreateResponse>{resp, err};
      });
}

Result<HttpRequestReceiver> TokenApi::ListTokens(ITokenRepository* repository) {
  auto [list, err] = repository->List();
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
  auto err = repository->Remove(std::string(name));
  return DefaultReceiver(err);
}
}  // namespace reduct::api
