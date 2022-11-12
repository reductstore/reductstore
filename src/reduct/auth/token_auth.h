// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TOKEN_AUTH_H
#define REDUCT_STORAGE_TOKEN_AUTH_H

#include <string>
#include <string_view>

#include "reduct/core/result.h"

namespace reduct::auth {

/**
 *  Trivial Token Authentication w/o encoded subject
 */
class ITokenAuthentication {
 public:
  enum class Roles {
    kFullAccess = 0,
    kRead = 1,
    kWrite = 2,
  };

  struct Options {};

  /**
   * @brief Check if the access token is valid
   * @param authorization_header The header with token
   * @return 200 if Ok
   */
  virtual core::Error Check(std::string_view authorization_header, Roles roles = Roles::kFullAccess,
                            std::string_view bucket = "") const = 0;

  static std::unique_ptr<ITokenAuthentication> Build(std::string_view api_token, Options options = Options{});
};
}  // namespace reduct::auth

#endif  // REDUCT_STORAGE_TOKEN_AUTH_H
