// Copyright 2021-2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
#include <uWebSockets/Loop.h>

#include <csignal>

#include "reduct/api/http_server.h"
#include "reduct/asset/asset_manager.h"
#include "reduct/async/loop.h"
#include "reduct/auth/token_auth.h"
#include "reduct/config.h"
#include "reduct/core/logger.h"
#include "reduct/storage/storage.h"

#ifdef WITH_CONSOLE
#include "reduct/console.h"
#endif
#include "rust/rust_part.h"

using reduct::api::IHttpServer;
using reduct::asset::IAssetManager;
using reduct::async::ILoop;
using reduct::auth::ITokenAuthorization;
using reduct::auth::ITokenRepository;
using reduct::core::Error;
using reduct::core::Logger;
using ReductStorage = reduct::storage::IStorage;

class Loop : public ILoop {
 public:
  void Defer(Task&& task) override { uWS::Loop::get()->defer(std::move(task)); }
};

static bool running = true;
static void SignalHandler(auto signal) { running = false; }

int main() {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  auto env = reduct::core::new_env();

  LOG_INFO("ReductStore {}", reduct::kVersion);

  auto log_level = env->get_string("RS_LOG_LEVEL", "INFO", false);
  auto host = env->get_string("RS_HOST", "0.0.0.0", false);
  auto port = env->get_int("RS_PORT", 8383, false);
  auto api_base_path = env->get_string("RS_API_BASE_PATH", "/", false);
  auto data_path = env->get_string("RS_DATA_PATH", "/data", false);
  auto api_token = env->get_string("RS_API_TOKEN", "", true);
  auto cert_path = env->get_string("RS_CERT_PATH", "", false);
  auto cert_key_path = env->get_string("RS_CERT_KEY_PATH", "", false);

  Logger::set_level(log_level.c_str());
  reduct::core::init_log(log_level);  // rust logger

  LOG_INFO("Configuration: \n {}", std::string(env->message()));

  Loop loop;
  ILoop::set_loop(&loop);

#if WITH_CONSOLE
  auto console = reduct::asset::new_asset_manager(rust::Str(reduct::kZippedConsole.data()));
#else
  auto console = reduct::asset::new_asset_manager(rust::Str(""));
#endif

  IHttpServer::Components components{
      .storage = ReductStorage::Build({.data_path = data_path.c_str()}),
      .auth = ITokenAuthorization::Build(api_token.c_str()),
      .token_repository = ITokenRepository::Build({.data_path = data_path.c_str(), .api_token = api_token.c_str()}),
      .console = std::move(console),
  };

  auto server = IHttpServer::Build(std::move(components), {
                                                              .host = host.c_str(),
                                                              .port = port,
                                                              .base_path = api_base_path.c_str(),
                                                              .cert_path = cert_path.c_str(),
                                                              .cert_key_path = cert_key_path.c_str(),
                                                          });
  return server->Run(running);
}
