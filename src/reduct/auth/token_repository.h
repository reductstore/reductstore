// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_TOKEN_REPOSITORY_H
#define REDUCT_STORAGE_TOKEN_REPOSITORY_H

#include <filesystem>
#include <memory>
#include <string_view>
#include <vector>

#include "reduct/core/error.h"
#include "reduct/core/result.h"
#include "reduct/proto/api/auth.pb.h"

namespace reduct::auth {

/**
 * Repository to manage API tokens
 */
class ITokenRepository {
 public:
  using TokenPermissions = proto::api::Token::Permissions;
  using Token = proto::api::Token;
  using TokenList = std::vector<Token>;
  using TokenCreateResponse = proto::api::TokenCreateResponse;

  virtual ~ITokenRepository() = default;
  /**
   * Create a new token
   * @param name  token name
   * @param permissions  permissions (see protobuf message)
   * @return generated value of created token, 409 if it already exists
   */
  virtual core::Result<TokenCreateResponse> CreateToken(std::string name, TokenPermissions permissions) = 0;

  /**
   * Update permissions of a token
   * @param name
   * @param permissions
   * @return
   */
  virtual core::Error UpdateToken(const std::string &name, TokenPermissions permissions) = 0;

  /**
   * List tokens
   * @note it shouldn't expose the values of the tokens and permissions
   * @return list of tokens
   */
  virtual core::Result<TokenList> GetTokenList() const = 0;

  /**
   * Find a token by its name
   * @note shouldn't expose the value of token, only permissions
   * @param name token name
   * @return token without value, 404 if not found
   */
  virtual core::Result<Token> FindByName(const std::string &name) const = 0;

  /**
   * Validate token
   * @note shouldn't expose the value of token, only permissions
   * @param value value of token
   * @return token without value, 404 if not found
   */
  virtual core::Result<Token> ValidateToken(std::string_view value) const = 0;

  /**
   * Remove a token by name
   * @param name token name
   * @return 404 if not found
   */
  virtual core::Error RemoveToken(const std::string &name) = 0;

  struct Options {
    std::filesystem::path data_path;
    std::string_view api_token;
  };

  /**
   * Load tokens from disk and build a repo
   * @param options
   * @return nullptr if failed
   */
  static std::unique_ptr<ITokenRepository> Build(Options options);
};

}  // namespace reduct::auth
#endif  // REDUCT_STORAGE_TOKEN_REPOSITORY_H
