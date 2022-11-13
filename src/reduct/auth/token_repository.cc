// Copyright 2022 Alexey Timin

#include "reduct/auth/token_repository.h"

#include <google/protobuf/util/time_util.h>

#include <fstream>
#include <map>
#include <random>
#include <ranges>

#include "reduct/core/logger.h"

namespace reduct::auth {

using core::Error;
using core::Result;
using proto::api::TokenRepo;

using google::protobuf::util::TimeUtil;

const std::string_view kTokenRepoFileName = ".auth";

class TokenRepository : public ITokenRepository {
 public:
  explicit TokenRepository(Options options) : repo_() {
    const auto token_repo_path = options.data_path / kTokenRepoFileName;
    std::ifstream file(token_repo_path);
    if (file) {
      TokenRepo probuf_repo;
      if (probuf_repo.ParseFromIstream(&file)) {
        LOG_WARNING("Failed to parse tokens");
        return;
      }

      LOG_DEBUG("Load {} tokens from {}", probuf_repo.tokens().size(), token_repo_path.string());
      for (const auto& token : probuf_repo.tokens()) {
        repo_[token.name()] = token;
      }
    }
  }

  Result<std::string> Create(std::string name, TokenPermisssions permissions) override {
    if (name.empty()) {
      return {{}, Error{.code = 422, .message = "Token name can't be empty"}};
    }

    if (!FindByName(name).error) {
      return {{}, Error{.code = 409, .message = fmt::format("Token '{}' already exists", name)}};
    }

    Token& new_token = repo_[name];
    new_token.set_name(std::move(name));
    new_token.mutable_created_at()->CopyFrom(TimeUtil::GetCurrentTime());
    new_token.mutable_permissions()->CopyFrom(permissions);

    auto random_hex_string = [](size_t length) {
      const std::string_view hex("0123456789abcdef");
      std::random_device dev;
      std::mt19937 rng(dev());
      std::uniform_int_distribution<std::mt19937::result_type> dist16(0, 15);

      std::string output(length, ' ');
      for (auto& chr : output) {
        chr = hex[dist16(rng)];
      }
      return output;
    };
    new_token.set_value(fmt::format("{}-{}", new_token.name(), random_hex_string(64)));

    return {new_token.value(), Error::kOk};
  }

  Result<TokenList> List() const override {
    TokenList token_list{};
    for (auto token : repo_ | std::views::values) {
      token.clear_permissions();  // we don't expose permissions and value when we do list
      token.clear_value();
      token_list.push_back(std::move(token));
    }

    return {token_list, Error::kOk};
  }

  Result<Token> FindByName(const std::string& name) const override {
    auto it = repo_.find(name);
    if (it == repo_.end()) {
      return {{}, Error{.code = 404, .message = fmt::format("Token '{}' doesn't exist", name)}};
    }

    Token token = it->second;
    token.clear_value();
    return {token, Error::kOk};
  }

  Result<Token> FindByValue(std::string_view value) const override { return core::Result<Token>(); }
  Error Remove(std::string_view name) override { return core::Error(); }

 private:
  std::map<std::string, Token> repo_;
};

std::unique_ptr<ITokenRepository> ITokenRepository::Build(ITokenRepository::Options options) {
  return std::make_unique<TokenRepository>(std::move(options));
}

}  // namespace reduct::auth
