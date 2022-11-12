// Copyright 2022 Alexey Timin

#include "reduct/auth/token_repository.h"

#include <google/protobuf/util/time_util.h>

#include <fstream>
#include <random>

#include "reduct/core/logger.h"

namespace reduct::auth {

using core::Error;
using proto::api::TokenRepo;

using google::protobuf::util::TimeUtil;

const std::string_view kTokenRepoFileName = ".auth";

class TokenRepository : public ITokenRepository {
 public:
  explicit TokenRepository(Options options) : repo_() {
    const auto token_repo_path = options.data_path / kTokenRepoFileName;
    std::ifstream token_repo(token_repo_path);
    if (token_repo) {
      repo_.ParseFromIstream(&token_repo);
      LOG_DEBUG("Load {} tokens from {}", repo_.tokens().size(), token_repo_path.string());
    }
  }

  core::Result<std::string> Create(std::string name, TokenPermisssions permissions) override {
    Token& new_token = *repo_.add_tokens();
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

  core::Result<TokenList> List() const override {
      TokenList token_list{};
      for (auto token : repo_.tokens()) {
        token.clear_permissions();  // we don't expose permissions and value when we do list
        token.clear_value();
        token_list.push_back(std::move(token));
      }

      return {token_list, Error::kOk};
  }

  core::Result<Token> FindtByName(std::string_view name) const override { return core::Result<Token>(); }
  core::Result<Token> FindByValue(std::string_view value) const override { return core::Result<Token>(); }
  core::Error Remove(std::string_view name) override { return core::Error(); }

 private:
  TokenRepo repo_;
};

std::unique_ptr<ITokenRepository> ITokenRepository::Build(ITokenRepository::Options options) {
  return std::make_unique<TokenRepository>(std::move(options));
}

}  // namespace reduct::auth
