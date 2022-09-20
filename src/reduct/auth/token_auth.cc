// Copyright 2022 Alexey Timin

#include "reduct/auth/token_auth.h"

#include <botan/hex.h>
#include <google/protobuf/util/time_util.h>

#include "reduct/core/logger.h"
#include "reduct/proto/api/auth.pb.h"

namespace reduct::auth {

using async::Run;
using core::Error;
using proto::api::RefreshTokenResponse;
using proto::api::Token;

using google::protobuf::util::TimeUtil;

/**
 * Does nothing
 */
class NoAuthentication : public ITokenAuthentication {
 public:
  Error Check(std::string_view authorization_header) const override { return Error::kOk; }
};

class BearerTokenAuthentication : public ITokenAuthentication {
 public:
  explicit BearerTokenAuthentication(std::string_view api_token) { api_token_ = api_token; }

  Error Check(std::string_view authorization_header) const override {
    if (!authorization_header.starts_with("Bearer ")) {
      return {.code = 401, .message = "No bearer token in request header"};
    }

    auto access_token = authorization_header.substr(7, authorization_header.size() - 7);
    if (access_token != api_token_) {
      return {.code = 401, .message = fmt::format("Invalid token")};
    }

    return Error::kOk;
  }

 private:
  std::string api_token_;
};

std::unique_ptr<ITokenAuthentication> ITokenAuthentication::Build(std::string_view api_token, Options options) {
  if (api_token.empty()) {
    LOG_WARNING("API token is empty. No authentication.");
    return std::make_unique<NoAuthentication>();
  }

  return std::make_unique<BearerTokenAuthentication>(api_token);
}

}  // namespace reduct::auth
