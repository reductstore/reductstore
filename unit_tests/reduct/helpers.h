// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#ifndef REDUCT_STORAGE_HELPERS_H
#define REDUCT_STORAGE_HELPERS_H

#include <fmt/core.h>
#include <google/protobuf/util/json_util.h>

#include <filesystem>
#include <random>

#include "reduct/core/common.h"
#include "reduct/proto/api/bucket.pb.h"
#include "reduct/storage/entry.h"
#include "reduct/storage/storage.h"

inline bool operator==(const google::protobuf::MessageLite& msg_a, const google::protobuf::MessageLite& msg_b) {
  return (msg_a.GetTypeName() == msg_b.GetTypeName()) && (msg_a.SerializeAsString() == msg_b.SerializeAsString());
}

namespace reduct::proto::api {
inline std::ostream& operator<<(std::ostream& os, const BucketInfo& msg) {
  std::string str;
  google::protobuf::util::MessageToJsonString(msg, &str);
  os << str;
  return os;
}
}  // namespace reduct::proto::api

/**
 * Build a directory in /tmp with random name
 * @return
 */
inline std::filesystem::path BuildTmpDirectory() {
  std::random_device r;

  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(1, 10000000);

  std::filesystem::path path = std::filesystem::temp_directory_path() / fmt::format("reduct_{}", uniform_dist(e1));
  std::filesystem::create_directories(path);
  return path;
}

namespace reduct {

static proto::api::BucketSettings MakeDefaultBucketSettings() {
  using proto::api::BucketSettings;

  BucketSettings settings;
  settings.set_max_block_size(1000);
  settings.set_quota_type(BucketSettings::NONE);
  settings.set_quota_size(10);
  settings.set_max_block_records(40);

  return settings;
}

/**
 * Simple writing a record in one step
 * @param entry
 * @param blob
 * @param ts
 * @return
 */
inline auto WriteOne(storage::IEntry& entry, std::string_view blob, core::Time ts,  // NOLINT
                     const storage::IEntry::LabelMap& labels = {}) {
  auto [ret, err] = entry.BeginWrite(ts, blob.size(), core::kContentTypeOctetStream, labels);
  if (err) {
    return err;
  }
  return ret->Write(blob);
}

/**
 * Simple reading a record in one step
 * @param entry
 * @param ts
 * @return
 */
inline core::Result<std::string> ReadOne(const storage::IEntry& entry, core::Time ts) {
  auto [reader, err] = entry.BeginRead(ts);
  if (err) {
    return {{}, err};
  }

  auto read_res = reader->Read();
  if (read_res.error) {
    return {{}, read_res.error};
  }

  return {read_res.result.data, core::Error::kOk};
}

}  // namespace reduct

#endif  // REDUCT_STORAGE_HELPERS_H
