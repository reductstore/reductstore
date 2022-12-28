// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_POLICIES_H
#define REDUCT_STORAGE_POLICIES_H

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/api/auth.pb.h"

namespace reduct::auth {

class IAuthorizationPolicy {
 public:
  virtual ~IAuthorizationPolicy() = default;

  using TokenPermissions = proto::api::Token::Permissions;
  /**
   * Check if token is valid
   * @param token
   * @return
   */
  virtual core::Error Validate(const core::Result<TokenPermissions>& authentication) const = 0;
};

class Anonymous : public IAuthorizationPolicy {
 public:
  core::Error Validate(const core::Result<TokenPermissions>& authentication) const override { return core::Error::kOk; }
};

class Authenticated : public IAuthorizationPolicy {
 public:
  core::Error Validate(const core::Result<TokenPermissions>& authentication) const override {
    return authentication.error;
  }
};

class FullAccess : public IAuthorizationPolicy {
 public:
  core::Error Validate(const core::Result<TokenPermissions>& authentication) const override;
};

class ReadAccess : public IAuthorizationPolicy {
 public:
  explicit ReadAccess(std::string bucket);
  core::Error Validate(const core::Result<TokenPermissions>& authentication) const override;

 private:
  std::string bucket_;
};

class WriteAccess : public IAuthorizationPolicy {
 public:
  explicit WriteAccess(std::string bucket);
  core::Error Validate(const core::Result<TokenPermissions>& authentication) const override;

 private:
  std::string bucket_;
};
}  // namespace reduct::auth

#endif  // REDUCT_STORAGE_POLICIES_H
