// Copyright 2022 Alexey Timin

#ifndef REDUCT_STORAGE_COMMON_H
#define REDUCT_STORAGE_COMMON_H

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
  std::function<core::Result<std::string>()> SendData;

  static HttpResponse Default() {
    return {
        {},
        0,
        []() {
          return core::Result<std::string>{"", core::Error::kOk};
        },
    };
  }
};

using HttpRequestReceiver = std::function<core::Result<HttpResponse>(std::string_view, bool)>;

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

static core::Result<HttpRequestReceiver> DefaultReceiver(core::Error error = core::Error::kOk) {
  return {
      [error = std::move(error)](std::string_view chunk, bool last) -> core::Result<HttpResponse> {
        return {HttpResponse::Default(), error};
      },
      core::Error::kOk,
  };
}

template <class T = google::protobuf::Message>
static core::Result<HttpRequestReceiver> SendJson(core::Result<T> &&res) {
  return core::Result<HttpRequestReceiver>{
      [data = std::move(res.result)](std::string_view, bool) mutable -> core::Result<HttpResponse> {
        auto json = PrintToJson(data);
        return {
            HttpResponse{
                .headers = {{"Content-Type", "application/json"}},
                .content_length = json.size(),
                .SendData =
                    [json = std::move(json)]() {
                      return core::Result<std::string>{json, core::Error::kOk};
                    },
            },
            core::Error::kOk,
        };
      },
      res.error,
  };
}

template <class T = google::protobuf::Message>
static core::Result<HttpRequestReceiver> ReceiveJson(std::function<core::Error(T &&)> handler) {
  using core::Error;
  using core::Result;
  using google::protobuf::util::JsonStringToMessage;

  auto buffer = std::make_shared<std::string>();
  buffer->reserve(1024);
  return Result<HttpRequestReceiver>{
      [data = std::move(buffer), handler = std::move(handler)](std::string_view chunk,
                                                               bool last) -> Result<HttpResponse> {
        data->append(chunk);
        if (!last) {
          return Result<HttpResponse>{HttpResponse::Default(), Error{.code = 100, .message = "Continue"}};
        }

        auto response = HttpResponse::Default();
        T proto_message;
        if (!data->empty()) {
          auto status = JsonStringToMessage(*data, &proto_message);
          if (!status.ok()) {
            return Result<HttpResponse>{
                response,
                Error{.code = 422, .message = fmt::format("Failed parse JSON buffer: {}", status.message().ToString())},
            };
          }
        }

        core::Error err = handler(std::move(proto_message));
        return Result<HttpResponse>{response, err};
      },
      Error::kOk,
  };
}

template <typename Rx, typename Tx>
static core::Result<HttpRequestReceiver> ReceiveAndSendJson(std::function<core::Result<Tx>(Rx &&)> handler) {
  using core::Error;
  using core::Result;
  using google::protobuf::util::JsonStringToMessage;

  auto buffer = std::make_shared<std::string>();
  buffer->reserve(1024);
  return Result{
      [data = std::move(buffer), handler = std::move(handler)](std::string_view chunk,
                                                               bool last) -> Result<HttpResponse> {
        data->append(chunk);
        if (!last) {
          return {HttpResponse::Default(), {.code = 100, .message = "Continue"}};
        }

        auto response = HttpResponse::Default();
        Rx proto_message;
        if (!data->empty()) {
          auto status = JsonStringToMessage(*data, &proto_message);
          if (!status.ok()) {
            return Result<HttpResponse>{
                response,
                Error{.code = 422, .message = fmt::format("Failed parse JSON buffer: {}", status.message().ToString())},
            };
          }
        }

        core::Result<Tx> result = handler(std::move(proto_message));
        auto json = PrintToJson(result.result);
        return {
            HttpResponse{
                .headers = {},
                .content_length = json.size(),
                .SendData =
                    [json = std::move(json)]() {
                      return core::Result<std::string>{json, core::Error::kOk};
                    },
            },
            result.error,
        };
      },
      Error::kOk,
  };
}

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_COMMON_H
