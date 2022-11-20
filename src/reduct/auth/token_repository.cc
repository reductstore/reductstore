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
    config_path_ = options.data_path / kTokenRepoFileName;
    std::ifstream file(config_path_, std::ios::binary);
    if (file) {
      TokenRepo proto_repo;
      if (!proto_repo.ParseFromIstream(&file)) {
        LOG_WARNING("Failed to parse tokens");
        return;
      }

      LOG_DEBUG("Load {} tokens from {}", proto_repo.tokens().size(), config_path_.string());
      for (const auto& token : proto_repo.tokens()) {
        repo_[token.name()] = token;
      }
    }

    if (!options.api_token.empty()) {
      const auto kInitTokenName = "init-token";
      auto v = repo_ | std::views::values |
               std::views::filter([&options](auto t) { return t.value() == options.api_token; });
      if (!v.empty()) {
        return;
      }

      LOG_DEBUG("Create '{}' token", kInitTokenName);
      Token token;
      token.set_name(kInitTokenName);
      token.set_value(std::string(options.api_token));
      token.mutable_created_at()->CopyFrom(TimeUtil::GetCurrentTime());

      token.mutable_permissions()->set_full_access(true);
      repo_[kInitTokenName] = token;

      if (auto err = SaveRepo()) {
        LOG_ERROR("Failed to save '{}': {}", kInitTokenName, err);
      }
    }
  }

  Result<TokenCreateResponse> Create(std::string name, TokenPermissions permissions) override {
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

    if (auto err = SaveRepo()) {
      return {{}, err};
    }

    TokenCreateResponse response;
    response.set_value(new_token.value());
    response.mutable_created_at()->CopyFrom(new_token.created_at());
    return {response, Error::kOk};
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

  Result<Token> ValidateToken(std::string_view value) const override {
    auto v = repo_ | std::views::values | std::views::filter([value](auto t) { return t.value() == value; });
    if (v.empty()) {
      return {{}, Error::Unauthorized("Invalid token")};
    }

    Token found_token = v.front();
    found_token.clear_value();
    return {found_token, Error::kOk};
  }

  Error Remove(const std::string& name) override {
    if (repo_.erase(name) == 0) {
      return Error{.code = 404, .message = fmt::format("Token '{}' doesn't exist", name)};
    }
    return SaveRepo();
  }

 private:
  Error SaveRepo() const {
    TokenRepo protbuf_repo;
    for (const auto& token : repo_ | std::views::values) {
      *protbuf_repo.add_tokens() = token;
    }

    std::ofstream file(config_path_, std::ios::binary);
    if (!file) {
      return {.code = 500,
              .message =
                  fmt::format("Failed to open file {} for writing: {}", config_path_.string(), std::strerror(errno))};
    }

    if (!protbuf_repo.SerializePartialToOstream(&file)) {
      return {.code = 500, .message = "Failed to save tokens"};
    }
    return Error::kOk;
  }

  std::filesystem::path config_path_;
  std::map<std::string, Token> repo_;
};

std::unique_ptr<ITokenRepository> ITokenRepository::Build(ITokenRepository::Options options) {
  return std::make_unique<TokenRepository>(std::move(options));
}

}  // namespace reduct::auth
