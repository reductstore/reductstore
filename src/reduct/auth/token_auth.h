// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TOKEN_AUTH_H
#define REDUCT_STORAGE_TOKEN_AUTH_H

#include <string>
#include <string_view>

#include "reduct/core/result.h"
#include "token_repository.h"

namespace reduct::auth {

class IAuthorizationPolicy {
 public:
  virtual ~IAuthorizationPolicy() = default;

  /**
   * Check if token is valid
   * @param token
   * @return
   */
  virtual core::Error Validate(const ITokenRepository& repository,
                               const core::Result<ITokenRepository::TokenPermissions>& authentication) const = 0;
};

class Anybody : public IAuthorizationPolicy {
 public:
  core::Error Validate(const ITokenRepository& repository,
                       const core::Result<ITokenRepository::TokenPermissions>& authentication) const override {
    return core::Error::kOk;
  }
};

class Authenticated : public IAuthorizationPolicy {
 public:
  core::Error Validate(const ITokenRepository& repository,
                       const core::Result<ITokenRepository::TokenPermissions>& authentication) const override {
    return authentication.error;
  }
};

class FullAccess : public IAuthorizationPolicy {
 public:
  core::Error Validate(const ITokenRepository& repository,
                       const core::Result<ITokenRepository::TokenPermissions>& authentication) const override;
};

class ReadAccess : public IAuthorizationPolicy {
 public:
  explicit ReadAccess(std::string bucket);
  core::Error Validate(const ITokenRepository& repository,
                       const core::Result<ITokenRepository::TokenPermissions>& authentication) const override;

 private:
  std::string bucket_;
};

class WriteAccess : public IAuthorizationPolicy {
 public:
  explicit WriteAccess(std::string bucket);
  core::Error Validate(const ITokenRepository& repository,
                       const core::Result<ITokenRepository::TokenPermissions>& authentication) const override;

 private:
  std::string bucket_;
};

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
