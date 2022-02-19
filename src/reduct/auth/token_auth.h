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
  virtual core::Result<std::string> RefreshToken(std::string_view api_token) const = 0;
  virtual core::Error Check(std::string_view access_token) const = 0;

  static std::unique_ptr<ITokenAuthentication> Build(std::string_view api_token);
};
}  // namespace reduct::auth

#endif  // REDUCT_STORAGE_TOKEN_AUTH_H
