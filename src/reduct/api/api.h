// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_API_H
#define REDUCT_STORAGE_API_H

#include <fmt/format.h>
#include <google/protobuf/util/json_util.h>

#include <functional>
#include <string>
#include <string_view>

#include "reduct/core/error.h"
#include "reduct/core/result.h"

namespace reduct::api {

using StringMap = std::unordered_map<std::string, std::string>;

struct HttpResponse {
  StringMap headers;
  size_t content_length;
  std::function<core::Error(std::string_view, bool)> input_call;
  std::function<core::Result<std::string>()> output_call;

  static HttpResponse Default() {
    return {
        {},
        0,
        [](std::string_view chunk, bool last) { return core::Error::kOk; },
        []() {
          return core::Result<std::string>{"", core::Error::kOk};
        },
    };
  }
};

using HttpResponseHandler = std::function<core::Result<HttpResponse>()>;

template <class T>
std::string PrintToJson(T &&msg) {
  using google::protobuf::util::JsonPrintOptions;
  using google::protobuf::util::MessageToJsonString;

  std::string data;
  JsonPrintOptions options;
  options.preserve_proto_field_names = true;
  options.always_print_primitive_fields = true;
  MessageToJsonString(msg, &data, options);
  return data;
}

template <class T>
static core::Result<HttpResponse> SendJson(core::Result<T> &&res) {
  using core::Error;
  using core::Result;

  auto [data, err] = res;
  auto json = err ? "" : PrintToJson(std::move(data));
  return {
      HttpResponse{
          {{"content-type", "application/json"}},
          json.size(),
          [](std::string_view chunk, bool last) { return Error::kOk; },
          [json = std::move(json), err = std::move(err)]() {
            return Result<std::string>{json, err};
          },
      },
      Error::kOk,
  };
}

template <class T>
static core::Result<HttpResponse> ReceiveJson(std::function<core::Error(T &&)> handler) {
  using core::Error;
  using core::Result;
  using google::protobuf::util::JsonStringToMessage;

  auto buffer = std::make_shared<std::string>();
  buffer->reserve(1024);
  return {
      HttpResponse{
          .headers = {},
          .content_length = 0,
          .input_call =
              [data = std::move(buffer), handler = std::move(handler)](std::string_view chunk, bool last) {
                data->append(chunk);
                if (last) {
                  T proto_message;
                  if (!data->empty()) {
                    auto status = JsonStringToMessage(*data, &proto_message);
                    if (!status.ok()) {
                      return Error{.code = 422,
                                   .message = fmt::format("Failed parse JSON buffer: {}", status.message().ToString())};
                    }
                  }

                  return handler(std::move(proto_message));
                }
                return Error::kOk;
              },
          .output_call = []() { return Result<std::string>{}; },
      },
      Error::kOk,
  };
}

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_API_H
