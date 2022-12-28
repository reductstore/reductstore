// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_TOKEN_AUTH_H
#define REDUCT_STORAGE_TOKEN_AUTH_H

#include <string>
#include <string_view>

#include "reduct/auth/policies.h"
#include "reduct/auth/token_repository.h"
#include "reduct/core/result.h"

namespace reduct::auth {

/**
 * Parse token from authorization header
 * @param authorization_header
 * @return
 */
core::Result<std::string> ParseBearerToken(std::string_view authorization_header);

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

  /**
   * @brief Build authorization instance
   * @param api_token
   * @param options
   * @return
   */
  static std::unique_ptr<ITokenAuthorization> Build(std::string_view api_token, Options options = Options{});
};
}  // namespace reduct::auth

#endif  // REDUCT_STORAGE_TOKEN_AUTH_H
