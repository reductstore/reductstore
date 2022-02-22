// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TOKEN_AUTH_H
#define REDUCT_STORAGE_TOKEN_AUTH_H

#include <string>
#include <string_view>

#include "reduct/api/callbacks.h"
#include "reduct/core/result.h"

namespace reduct::auth {

/**
 *  Trivial Token Authentication w/o encoded subject
 */
class ITokenAuthentication : public api::IRefreshToken {
 public:
  struct Options {
    int expiration_time_s;
  };

  /**
   * @brief Check if the access token is valid
   * @param authorization_header The header with token
   * @return 200 if Ok
   */
  virtual core::Error Check(std::string_view authorization_header) const = 0;

  static std::unique_ptr<ITokenAuthentication> Build(std::string_view api_token,
                                                     Options options = Options{.expiration_time_s = 300});
};
}  // namespace reduct::auth

#endif  // REDUCT_STORAGE_TOKEN_AUTH_H
