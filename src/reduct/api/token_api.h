// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TOKEN_API_H
#define REDUCT_STORAGE_TOKEN_API_H

#include "reduct/api/common.h"
#include "reduct/auth/token_repository.h"

namespace reduct::api {

class TokenApi {
 public:
  /**
   * POST /tokens/:name
   * @param repository
   * @param name
   * @return
   */
  static core::Result<HttpRequestReceiver> CreateToken(auth::ITokenRepository* repository, std::string_view name);
};

}  // namespace reduct::api
#endif  // REDUCT_STORAGE_TOKEN_API_H
