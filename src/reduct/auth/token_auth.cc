// Copyright 2022 Alexey Timin

#include "reduct/auth/token_auth.h"

#include <google/protobuf/util/time_util.h>

#include <ranges>
#include <utility>

#include "reduct/core/logger.h"
#include "reduct/proto/api/auth.pb.h"

namespace reduct::auth {

using core::Error;
using google::protobuf::util::TimeUtil;

/**
 * Does nothing
 */
class NoAuthentication : public ITokenAuthorization {
 public:
  Error Check(std::string_view authorization_header, const ITokenRepository& repository,
              const IAuthorizationPolicy& policy) const override {
    return Error::kOk;
  }
};

class BearerTokenAuthentication : public ITokenAuthorization {
 public:
  BearerTokenAuthentication() = default;

  Error Check(std::string_view authorization_header, const ITokenRepository& repository,
              const IAuthorizationPolicy& policy) const override {
    if (!authorization_header.starts_with("Bearer ")) {
      return policy.Validate({{}, Error::Unauthorized("No bearer token in request header")});
    }

    auto token_value = authorization_header.substr(7, authorization_header.size() - 7);

    auto [token, error] = repository.ValidateToken(token_value);
    if (error) {
      return error;
    }

    return policy.Validate({token.permissions(), Error::kOk});
  }
};

std::unique_ptr<ITokenAuthorization> ITokenAuthorization::Build(std::string_view api_token, Options options) {
  if (api_token.empty()) {
    LOG_WARNING("API token is empty. No authentication.");
    return std::make_unique<NoAuthentication>();
  }

  return std::make_unique<BearerTokenAuthentication>();
}

}  // namespace reduct::auth
