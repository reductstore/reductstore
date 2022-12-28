// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "reduct/auth/policies.h"

namespace reduct::auth {

using core::Error;

Error FullAccess::Validate(const core::Result<TokenPermissions>& authentication) const {
  auto [permissions, error] = authentication;
  if (error) {
    return error;
  }

  if (!permissions.full_access()) {
    return Error::Forbidden("Token doesn't have full access");
  }
  return Error::kOk;
}

ReadAccess::ReadAccess(std::string bucket) : bucket_(std::move(bucket)) {}

Error ReadAccess::Validate(const core::Result<TokenPermissions>& authentication) const {
  auto [permissions, error] = authentication;
  if (error) {
    return error;
  }

  if (permissions.full_access()) {
    return Error::kOk;
  }

  if (!std::ranges::any_of(permissions.read(), [this](const auto& bucket) { return bucket == bucket_; })) {
    return Error::Forbidden("Token doesn't have read access to bucket '" + bucket_ + "'");
  }

  return Error::kOk;
}

WriteAccess::WriteAccess(std::string bucket) : bucket_(std::move(bucket)) {}

core::Error WriteAccess::Validate(const core::Result<TokenPermissions>& authentication) const {
  auto [permissions, error] = authentication;
  if (error) {
    return error;
  }

  if (permissions.full_access()) {
    return Error::kOk;
  }

  if (!std::ranges::any_of(permissions.write(), [this](const auto& bucket) { return bucket == bucket_; })) {
    return Error::Forbidden("Token doesn't have write access to bucket '" + bucket_ + "'");
  }

  return Error::kOk;
}
}  // namespace reduct::auth
#include "policies.h"
