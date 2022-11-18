// Copyright 2022 Alexey Timin

#include "token_api.h"

namespace reduct::api {

using auth::ITokenRepository;
using core::Error;
using core::Result;
using reduct::proto::api::TokenCreateResponse;

core::Result<HttpRequestReceiver> TokenApi::CreateToken(ITokenRepository *repository, std::string_view name) {
  return DefaultReceiver();
  //  return ReceiveAndSendJson<ITokenRepository::TokenPermissions, TokenCreateResponse>(
  //      [repository, name = std::string(name)](ITokenRepository::TokenPermissions&& permissions) ->
  //      Result<TokenCreateResponse> {
  //        auto [token, err] = repository->Create(name, permissions);
  //        if (err) {
  //          return Result<TokenCreateResponse>{{}, err};
  //        }
  //
  //        TokenCreateResponse resp;
  //        resp.set_value(token);
  //        resp.PrintDebugString();
  //        return Result<TokenCreateResponse>{resp, Error::kOk};
  //      });
}
}  // namespace reduct::api
