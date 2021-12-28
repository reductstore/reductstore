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
  auto api_base_path = env.Get<std::string>("API_BASE_PATH", "/");
  auto data_path = env.Get<std::string>("DATA_PATH", "/var/reduct-storage/data");

  LOG_INFO("Configuration: \n {}", env.Print());

  auto storage = IStorage::Build({
      .data_path = data_path,
  });

  auto server = IApiServer::Build(std::move(storage), {
                                                          .host = host,
                                                          .port = port,
                                                          .base_path = api_base_path,
                                                      });
  server->Run();

  return 0;
}
