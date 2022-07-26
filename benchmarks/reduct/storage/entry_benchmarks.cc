// Copyright 2022 Alexey Timin

#define CATCH_CONFIG_ENABLE_BENCHMARKING
#include <catch2/catch.hpp>
#include <google/protobuf/util/time_util.h>

#include <filesystem>

#include "reduct/storage/bucket.h"
#include "reduct/storage/entry.h"

namespace fs = std::filesystem;

using reduct::core::Time;
using reduct::core::Error;
using reduct::proto::api::BucketSettings;
using reduct::storage::IBucket;
using reduct::storage::IEntry;

using google::protobuf::util::TimeUtil;

TEST_CASE("storage::IEntry write operation") {
  auto dir_path = fs::temp_directory_path() / "reduct" / "bucket";
  fs::remove_all(dir_path);

  BucketSettings settings;
  auto bucket = IBucket::Build(dir_path, settings);
  auto entry = bucket->GetOrCreateEntry("entry-1").entry.lock();

  for (int i = 0; i < 10000; ++i) {
    auto [writer, err] = entry->BeginWrite(Time::clock::now(), 10);
    [[maybe_unused]] auto ret = writer->Write("1234567890");
  }
  BENCHMARK("Write forward") {
    auto [writer, err] = entry->BeginWrite(Time::clock::now(), 10);
    [[maybe_unused]] auto ret = writer->Write("1234567890");
  };
}
