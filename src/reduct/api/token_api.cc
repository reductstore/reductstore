// Copyright 2022 Alexey Timin

#include "token_api.h"

namespace reduct::api {

using auth::ITokenRepository;
using core::Error;
using core::Result;
using reduct::proto::api::TokenCreateResponse;
using reduct::proto::api::Token_Permissions;

core::Result<HttpRequestReceiver> TokenApi::CreateToken(ITokenRepository* repository, std::string_view name) {
  return ReceiveAndSendJson<Token_Permissions, TokenCreateResponse>(
      [repository, name = std::string(name)](Token_Permissions&& permissions) -> Result<TokenCreateResponse> {
        auto [token, err] = repository->Create(name, permissions);

        TokenCreateResponse resp;
        resp.set_value(token);
        return Result<TokenCreateResponse>{resp, err};
      });
}
}  // namespace reduct::api
