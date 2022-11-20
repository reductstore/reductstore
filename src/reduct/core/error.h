// Copyright 2021 Alexey Timin

#ifndef REDUCT_CORE_ERROR_H
#define REDUCT_CORE_ERROR_H

#include <ostream>
#include <string>
#include <variant>

namespace reduct::core {

/**
 * Error with code and message
 */
struct [[nodiscard]] Error {  // NOLINT
  int code = 200;
  std::string message{};

  /**
   * true if there is an error
   * @return
   */
  operator bool() const;

  std::string ToString() const;

  std::strong_ordering operator<=>(const Error& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& os, const Error& error);

  /**
   * Use Error::kOk to avoid creating an object
   */
  static const Error kOk;

  enum Codes {
    kContinue = 100,
    kBadRequest = 400,
    kUnauthorized = 401,
    kForbidden = 403,
    kNotFound = 404,
    kMethodNotAllowed = 405,
    kConflict = 409,
    kContentLengthRequired = 411,
    kUnprocessableEntity = 422,
    kPreconditionFailed = 412,
    kInternalError = 500,
    kNotImplemented = 501,
    kBadGateway = 502,
    kServiceUnavailable = 503,
    kGatewayTimeout = 504,
  };

  // HTTP codes 100-200
  static Error Continue(std::string msg = "Continue") { return Error{Codes::kContinue, std::move(msg)}; }

  // HTTP codes 200-300
  // HTTP codes 300-400
  // HTTP codes 400-500
  static Error BadRequest(std::string msg = "Bad Request") { return Error{Codes::kBadRequest, std::move(msg)}; }
  static Error Unauthorized(std::string msg = "Unauthorized") { return Error{Codes::kUnauthorized, std::move(msg)}; }
  static Error Forbidden(std::string msg = "Forbidden") { return Error{Codes::kForbidden, std::move(msg)}; }
  static Error NotFound(std::string msg = "NotFound") { return Error{kNotFound, std::move(msg)}; }
  static Error Conflict(std::string msg = "Conflict") { return Error{kConflict, std::move(msg)}; }
  static Error ContentLengthRequired(std::string msg = "Content Length Required") {
    return Error{kContentLengthRequired, std::move(msg)};
  }
  static Error UnprocessableEntity(std::string msg = "Unprocessable Entity") {
    return Error{kUnprocessableEntity, std::move(msg)};
  }

  // HTTP codes 500-600
  static Error InternalError(std::string msg = "Internal Error") { return Error{kInternalError, std::move(msg)}; }
};

}  // namespace reduct::core
#endif  // REDUCT_CORE_ERROR_H
