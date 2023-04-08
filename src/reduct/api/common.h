// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef REDUCT_STORAGE_COMMON_H
#define REDUCT_STORAGE_COMMON_H

#include <fmt/format.h>
#include <google/protobuf/util/json_util.h>

#include <functional>
#include <string>
#include <string_view>

#include "../../../cmake-build-release/rust/rust_part.h"
#include "reduct/core/error.h"
#include "reduct/core/result.h"

namespace reduct::api {

using StringMap = std::unordered_map<std::string, std::string>;

/**
 * A result of handling of HTTP request
 */
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

/**
 * @brief HTTP request receiver
 * This function is receiver for a chuck of dat from uWS engine. If it receives the last chuck, it returns HttpResponse
 * with a function HttpResponse::SendData to send the response.
 */
using HttpRequestReceiver = std::function<core::Result<HttpResponse>(std::string_view, bool)>;

/**
 * A helper function to print a protobuf message as JSON
 * @tparam T
 * @param msg protobuf message
 * @return JSON string
 */
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

/**
 * Default receiver which does nothing but generate a response
 * @return
 */
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

static core::Result<HttpRequestReceiver> SendJson(core::Result<rust::String> &&res) {
  return core::Result<HttpRequestReceiver>{
      [data = std::move(res.result)](std::string_view, bool) mutable -> core::Result<HttpResponse> {
        return {
            HttpResponse{
                .headers = {{"Content-Type", "application/json"}},
                .content_length = data.size(),
                .SendData =
                    [json = std::move(data)]() {
                      return core::Result<std::string>{std::string(json.data(), json.size()), core::Error::kOk};
                    },
            },
            core::Error::kOk,
        };
      },
      res.error,
  };
}

template <class T = google::protobuf::Message>
static core::Result<T> CollectDataAndParseProtobuf(std::string_view chunk, bool last, std::string *buffer) {
  using google::protobuf::util::JsonStringToMessage;

  buffer->append(chunk);
  if (!last) {
    return core::Result<T>{T{}, core::Error::Continue()};
  }

  T proto_message;
  if (!buffer->empty()) {
    auto status = JsonStringToMessage(*buffer, &proto_message);
    if (!status.ok()) {
      return core::Result<T>{
          T{},
          core::Error{.code = 422, .message = fmt::format("Failed parse JSON buffer: {}", status.message().ToString())},
      };
    }
  }

  return core::Result<T>{proto_message, core::Error::kOk};
}

template <class T = google::protobuf::Message>
static core::Result<HttpRequestReceiver> ReceiveJson(std::function<core::Error(T &&)> handler) {
  using core::Error;
  using core::Result;

  auto buffer = std::make_shared<std::string>();
  buffer->reserve(1024);
  return Result<HttpRequestReceiver>{
      [data = std::move(buffer), handler = std::move(handler)](std::string_view chunk,
                                                               bool last) -> Result<HttpResponse> {
        auto [proto_message, error] = CollectDataAndParseProtobuf<T>(chunk, last, data.get());
        if (error.code != 200) {
          return {HttpResponse::Default(), error};
        }

        error = handler(std::move(proto_message));
        return {HttpResponse::Default(), error};
      },
      Error::kOk,
  };
}

template <typename Rx = google::protobuf::Message, typename Tx = google::protobuf::Message>
static core::Result<HttpRequestReceiver> ReceiveAndSendJson(std::function<core::Result<Tx>(Rx &&)> handler) {
  using core::Error;
  using core::Result;

  auto buffer = std::make_shared<std::string>();
  buffer->reserve(1024);
  return Result<HttpRequestReceiver>{
      [data = std::move(buffer), handler](std::string_view chunk, bool last) -> Result<HttpResponse> {
        auto [proto_message, error] = CollectDataAndParseProtobuf<Rx>(chunk, last, data.get());
        if (error.code != 200) {
          return {HttpResponse::Default(), error};
        }

        core::Result<Tx> result = handler(std::move(proto_message));
        if (result.error) {
          return Result<HttpResponse>(HttpResponse::Default(), result.error);
        }

        auto json = PrintToJson(result.result);
        return Result<HttpResponse>{
            HttpResponse{
                .headers = {{"Content-Type", "application/json"}},
                .content_length = json.size(),
                .SendData =
                    [json = std::move(json)]() {
                      return core::Result<std::string>{json, core::Error::kOk};
                    },
            },
            Error::kOk,
        };
      },
      Error::kOk,
  };
}

static core::Result<HttpRequestReceiver> ReceiveAndSendJson(
    std::function<core::Result<rust::String>(rust::String &&)> handler) {
  using core::Error;
  using core::Result;

  auto buffer = std::make_shared<std::string>();
  buffer->reserve(1024);
  return Result<HttpRequestReceiver>{
      [data = std::move(buffer), handler](std::string_view chunk, bool last) -> Result<HttpResponse> {
        data->append(chunk);
        if (!last) {
          return {HttpResponse::Default(), Error::Continue()};
        }

        auto [resp, err] = handler(std::move(rust::String(data->data(), data->size())));
        RETURN_ERROR(err);

        return Result<HttpResponse>{
            HttpResponse{
                .headers = {{"Content-Type", "application/json"}},
                .content_length = resp.size(),
                .SendData = [json = std::move(resp)]() { return std::string(json.data(), json.size()); },
            },
            Error::kOk,
        };
      },
      Error::kOk,
  };
}

/**
 * @brief HTTP request handler
 * This function is called by uWS engine when it receives a request. It returns a function to send a response.
 */
std::map<std::string, std::string> ParseQueryString(std::string_view query);

}  // namespace reduct::api

#endif  // REDUCT_STORAGE_COMMON_H
