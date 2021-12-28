// Copyright 2021 Alexey Timin

#ifndef REDUCT_STORAGE_HELPERS_H
#define REDUCT_STORAGE_HELPERS_H

#include <fmt/core.h>

#include <filesystem>
#include <random>

/**
 * Build a directory in /tmp with random name
 * @return
 */
inline std::filesystem::path BuildTmpDirectory() {
  std::random_device r;

  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(1, 100000);

  std::filesystem::path path = std::filesystem::temp_directory_path() / fmt::format("reduct_{}", uniform_dist(e1));
  std::filesystem::create_directories(path);
  return path;
}

#endif  // REDUCT_STORAGE_HELPERS_H
