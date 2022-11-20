// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TOKEN_AUTH_H
#define REDUCT_STORAGE_TOKEN_AUTH_H

#include <string>
#include <string_view>

#include "reduct/auth/policies.h"
#include "reduct/auth/token_repository.h"
#include "reduct/core/result.h"

namespace reduct::auth {

/**
 *  Authorization by token
 */
class ITokenAuthorization {
 public:
  struct Options {};

  virtual ~ITokenAuthorization() = default;
  /**
   * @brief Check if the access token is valid
   * @param authorization_header The header with token
   * @param repository repository of tokens
   * @param policy authorization policy
   * @return 200 if Ok
   */
  virtual core::Error Check(std::string_view authorization_header, const ITokenRepository& repository,
                            const IAuthorizationPolicy& policy) const = 0;

  static std::unique_ptr<ITokenAuthorization> Build(std::string_view api_token, Options options = Options{});
};
}  // namespace reduct::auth

#endif  // REDUCT_STORAGE_TOKEN_AUTH_H
