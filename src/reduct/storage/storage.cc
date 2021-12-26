// Copyright 2021 Alexey Timin
#include "reduct/storage/storage.h"

namespace reduct::storage {

using core::Error;

class Storage : public IStorage {
 public:
  explicit Storage(Options options) : options_(options) {}

  [[nodiscard]] std::unique_ptr<api::IApiHandler> BindWithApi() const override {
    class ApiHandler : public reduct::api::IApiHandler {
     public:
      [[nodiscard]] Error OnInfoRequest(InfoResponse *res) const override {
        return Error{.code = 500, .message = "Failed"};
      }
    };

    return std::make_unique<ApiHandler>();
  }

 private:
  Options options_;
};

std::unique_ptr<IStorage> IStorage::Build(IStorage::Options options) {
  return std::make_unique<Storage>(std::move(options));
}

}  // namespace reduct::storage