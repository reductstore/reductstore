// Copyright 2021 Alexey Timin
#include <uWebSockets/Loop.h>

#include <csignal>

#include "reduct/api/api_server.h"
#include "reduct/async/loop.h"
#include "reduct/auth/token_auth.h"
#include "reduct/config.h"
#include "reduct/core/env_variable.h"
#include "reduct/core/logger.h"
#include "reduct/storage/storage.h"

using reduct::api::IApiServer;
using reduct::async::ILoop;
using reduct::auth::ITokenAuthentication;
using reduct::core::EnvVariable;
using reduct::core::Error;
using reduct::core::Logger;
using reduct::storage::IStorage;

class Loop : public ILoop {
 public:
  void Defer(Task&& task) override { uWS::Loop::get()->defer(std::move(task)); }
};

static bool running = true;
static void SignalHandler(auto signal) { running = false; }

int main() {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  LOG_INFO("Reduct Storage {}", reduct::kVersion);

  EnvVariable env;
  auto log_level = env.Get<std::string>("RS_LOG_LEVEL", "INFO");
  auto host = env.Get<std::string>("RS_HOST", "0.0.0.0");
  auto port = env.Get<int>("RS_PORT", 8383);
  auto api_base_path = env.Get<std::string>("RS_API_BASE_PATH", "/");
  auto data_path = env.Get<std::string>("RS_DATA_PATH", "/data");
  auto api_token = env.Get<std::string>("RS_API_TOKEN", "", true);

  Logger::set_level(log_level);

  LOG_INFO("Configuration: \n {}", env.Print());

  Loop loop;
  ILoop::set_loop(&loop);

  IApiServer::Components components{
      .handler = IStorage::Build({.data_path = data_path}),
      .auth = ITokenAuthentication::Build(api_token),
  };

  auto server = IApiServer::Build(std::move(components), {.host = host, .port = port, .base_path = api_base_path});
  server->Run(running);

  return 0;
}
