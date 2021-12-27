// Copyright 2021 Alexey Timin
#include "reduct/api/api_server.h"
#include "reduct/config.h"
#include "reduct/core/env_variable.h"
#include "reduct/core/logger.h"
#include "reduct/storage/storage.h"

using reduct::api::IApiServer;
using reduct::core::EnvVariable;
using reduct::core::Error;
using reduct::storage::IStorage;

int main() {
  LOG_INFO("Reduct Storage {}", reduct::kVersion);

  EnvVariable env;
  auto host = env.Get<std::string>("HOST", "0.0.0.0");
  auto port = env.Get<int>("PORT", 8383);
  auto base_path = env.Get<std::string>("BASE_PATH", "/");

  LOG_INFO("Configuration: \n {}", env.Print());

  auto storage = IStorage::Build({});
  auto server = IApiServer::Build(storage->BindWithApi(), {
                                                                      .host = host,
                                                                      .port = port,
                                                                      .base_path = base_path,
                                                                  });
  server->Run();

  return 0;
}
