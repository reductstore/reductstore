// Copyright 2022 Alexey Timin

#include "reduct/auth/token_auth.h"

#include <botan/auto_rng.h>
#include <botan/cipher_mode.h>
#include <botan/hash.h>
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

  [[nodiscard]] async::Run<ITokenAuthentication::Result> OnRefreshToken(
      const ITokenAuthentication::Request& req) const override {
    using Result = ITokenAuthentication::Result;
    return Run<Result>([] { return Result{{}, Error::kOk}; });
  }
};

class JwtAuthentication : public ITokenAuthentication {
 public:
  explicit JwtAuthentication(std::string_view api_token, Options options) : options_(std::move(options)) {
    // generate fresh nonce (IV)
    Botan::AutoSeeded_RNG rng;
    iv_ = rng.random_vec(16);
    key_ = HashToken(api_token);
  }

  Error Check(std::string_view authorization_header) const override {
    if (!authorization_header.starts_with("Bearer ")) {
      return {.code = 401, .message = "No bearer token in response header"};
    }

    auto access_token = authorization_header.substr(7, authorization_header.size() - 7);

    Botan::SecureVector<uint8_t> input_key;
    try {
      input_key = Botan::hex_decode_locked(access_token.data(), access_token.size());

      std::unique_ptr<Botan::Cipher_Mode> dec = Botan::Cipher_Mode::create("AES-256/CBC/PKCS7", Botan::DECRYPTION);
      dec->set_key(key_);
      dec->start(iv_);
      dec->finish(input_key);
    } catch (const std::exception& err) {
      return {.code = 401, .message = fmt::format("Invalid token")};
    }

    Token token;
    if (!token.ParseFromArray(input_key.data(), input_key.size())) {
      return {.code = 401, .message = "Invalid token"};
    }

    if (token.expired_at() < TimeUtil::GetCurrentTime()) {
      return {.code = 401, .message = "Expired token"};
    }

    return Error::kOk;
  }

  [[nodiscard]] Run<Result> OnRefreshToken(const Request& req) const override {
    return async::Run<Result>([this, api_token = req] {
      if (!api_token.starts_with("Bearer ")) {
        return Result{{}, Error{.code = 401, .message = "No bearer token in response header"}};
      }

      try {
        if (key_ != Botan::hex_decode(api_token.substr(7, api_token.size() - 7))) {
          throw;
        }
      } catch (...) {
        return Result{{}, Error{.code = 401, .message = "Invalid API token"}};
      }

      Token token;
      token.mutable_expired_at()->CopyFrom(TimeUtil::GetCurrentTime() +
                                           TimeUtil::SecondsToDuration(options_.expiration_time_s));
      auto payload = token.SerializeAsString();

      Botan::SecureVector<uint8_t> input_key(payload.begin(), payload.end());

      std::unique_ptr<Botan::Cipher_Mode> enc = Botan::Cipher_Mode::create("AES-256/CBC/PKCS7", Botan::ENCRYPTION);
      enc->set_key(key_);
      enc->start(iv_);
      enc->finish(input_key);

      RefreshTokenResponse resp;
      resp.set_expired_at(token.expired_at().seconds());
      resp.set_access_token(Botan::hex_encode(input_key.data(), input_key.size(), false));

      return Result{resp, Error::kOk};
    });
  }

 private:
  static Botan::OctetString HashToken(const std::string_view& api_token) {
    auto hash = Botan::HashFunction::create("SHA-256");
    hash->update(std::string(api_token));
    return hash->final();
  }

  Options options_;
  Botan::OctetString key_;
  Botan::SecureVector<uint8_t> iv_;
};

std::unique_ptr<ITokenAuthentication> ITokenAuthentication::Build(std::string_view api_token, Options options) {
  if (api_token.empty()) {
    LOG_WARNING("API token is empty. No authentication.");
    return std::make_unique<NoAuthentication>();
  }

  return std::make_unique<JwtAuthentication>(api_token, std::move(options));
}

}  // namespace reduct::auth
