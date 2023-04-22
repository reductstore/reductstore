// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::time::SystemTimeError;

/// HTTP status codes.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum HTTPStatus {
    OK = 200,
    Continue = 100,
    Created = 201,
    Accepted = 202,
    NoContent = 204,
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    NotAcceptable = 406,
    RequestTimeout = 408,
    Conflict = 409,
    Gone = 410,
    LengthRequired = 411,
    PreconditionFailed = 412,
    PayloadTooLarge = 413,
    URITooLong = 414,
    UnsupportedMediaType = 415,
    RangeNotSatisfiable = 416,
    ExpectationFailed = 417,
    ImATeapot = 418,
    MisdirectedRequest = 421,
    UnprocessableEntity = 422,
    Locked = 423,
    FailedDependency = 424,
    TooEarly = 425,
    UpgradeRequired = 426,
    PreconditionRequired = 428,
    TooManyRequests = 429,
    RequestHeaderFieldsTooLarge = 431,
    UnavailableForLegalReasons = 451,
    InternalServerError = 500,
    NotImplemented = 501,
    BadGateway = 502,
    ServiceUnavailable = 503,
    GatewayTimeout = 504,
    HTTPVersionNotSupported = 505,
    VariantAlsoNegotiates = 506,
    InsufficientStorage = 507,
    LoopDetected = 508,
    NotExtended = 510,
    NetworkAuthenticationRequired = 511,
}

/// An HTTP error, we use it for error handling.
#[derive(PartialEq, Debug)]
pub struct HTTPError {
    /// The HTTP status code.
    pub status: HTTPStatus,

    /// The human readable message.
    pub message: String,
}

impl Display for HTTPError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "[{:?}] {}", self.status, self.message)
    }
}

impl From<std::io::Error> for HTTPError {
    fn from(err: std::io::Error) -> Self {
        // An IO error is an internal server error
        HTTPError {
            status: HTTPStatus::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl From<SystemTimeError> for HTTPError {
    fn from(err: SystemTimeError) -> Self {
        // A system time error is an internal server error
        HTTPError {
            status: HTTPStatus::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl HTTPError {
    pub fn new(status: HTTPStatus, message: &str) -> Self {
        HTTPError {
            status,
            message: message.to_string(),
        }
    }

    pub fn status(&self) -> i32 {
        self.status as i32
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn ok() -> HTTPError {
        HTTPError {
            status: HTTPStatus::OK,
            message: "".to_string(),
        }
    }

    /// Create a not found error.
    pub fn not_found(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::NotFound,
            message: msg.to_string(),
        }
    }

    /// Create a conflict error.
    pub fn conflict(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::Conflict,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn bad_request(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::BadRequest,
            message: msg.to_string(),
        }
    }

    /// Create an unauthorized error.
    pub fn unauthorized(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::Unauthorized,
            message: msg.to_string(),
        }
    }

    /// Create a forbidden error.
    pub fn forbidden(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::Forbidden,
            message: msg.to_string(),
        }
    }

    /// Create an unprocessable entity error.
    pub fn unprocessable_entity(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::UnprocessableEntity,
            message: msg.to_string(),
        }
    }

    /// Create a too early error.
    pub fn too_early(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::TooEarly,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn internal_server_error(msg: &str) -> HTTPError {
        HTTPError {
            status: HTTPStatus::InternalServerError,
            message: msg.to_string(),
        }
    }
}
