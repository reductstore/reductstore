// Copyright 2022 Alexey Timin

#include "reduct/auth/token_auth.h"

#include "reduct/core/logger.h"

namespace reduct::auth {

using core::Error;

/**
 * Does nothing
 */
class NoAuthentication : public ITokenAuthentication {
 public:
  core::Result<std::string> RefreshToken(std::string_view api_token) const override { return {{}, Error::kOk}; }
  core::Error Check(std::string_view access_token) const override { return Error::kOk; }
};

std::unique_ptr<ITokenAuthentication> ITokenAuthentication::Build(std::string_view api_token) {
  LOG_INFO("API token is empty. No authentication.");
  return std::unique_ptr<NoAuthentication>();
}

}  // namespace reduct::auth