// Copyright 2021 Alexey Timin
#ifndef REDUCT_STORAGE_STORAGE_H
#define REDUCT_STORAGE_STORAGE_H

#include <filesystem>

#include "reduct/proto/api/server.pb.h"
#include "reduct/storage/bucket.h"

namespace reduct::storage {

/**
 * Data Storage
 */
class IStorage {
 public:
  struct Options {
    std::filesystem::path data_path;
  };

  virtual ~IStorage() = default;

  /**
   * Returns statistics about storage
   * @return
   */
  virtual core::Result<proto::api::ServerInfo> GetInfo() const = 0;

  /***
   * Returns list of buckets
   * @return
   */
  virtual core::Result<proto::api::BucketInfoList> GetList() const = 0;

  /**
   * Creates a new bucket
   * @param bucket_name
   * @param settings
   * @return
   */
  virtual core::Error CreateBucket(const std::string& bucket_name, const proto::api::BucketSettings& settings) = 0;

  /**
   * Gets a bucket
   * @param bucket_name
   * @return
   */
  virtual core::Result<IBucket::WPtr> GetBucket(const std::string& bucket_name) const = 0;

  /**
   * Remove a bucket
   * @param bucket_name
   * @return
   */
  virtual core::Error RemoveBucket(const std::string& bucket_name) = 0;

  /**
   * Build storage
   * @param options
   * @return
   */
  static std::unique_ptr<IStorage> Build(Options options);
};

}  // namespace reduct::storage

#endif  // REDUCT_STORAGE_STORAGE_H
