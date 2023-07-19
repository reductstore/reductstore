// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::de::StdError;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::time::SystemTimeError;

/// HTTP status codes.
#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
pub enum HttpStatus {
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
pub struct HttpError {
    /// The HTTP status code.
    pub status: HttpStatus,

    /// The human readable message.
    pub message: String,
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "[{:?}] {}", self.status, self.message)
    }
}

impl From<std::io::Error> for HttpError {
    fn from(err: std::io::Error) -> Self {
        // An IO error is an internal reductstore error
        HttpError {
            status: HttpStatus::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl From<SystemTimeError> for HttpError {
    fn from(err: SystemTimeError) -> Self {
        // A system time error is an internal reductstore error
        HttpError {
            status: HttpStatus::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl HttpError {
    pub fn new(status: HttpStatus, message: &str) -> Self {
        HttpError {
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

    pub fn ok() -> HttpError {
        HttpError {
            status: HttpStatus::OK,
            message: "".to_string(),
        }
    }

    /// Create a no content error.
    pub fn no_content(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::NoContent,
            message: msg.to_string(),
        }
    }

    /// Create a not found error.
    pub fn not_found(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::NotFound,
            message: msg.to_string(),
        }
    }

    /// Create a conflict error.
    pub fn conflict(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::Conflict,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn bad_request(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::BadRequest,
            message: msg.to_string(),
        }
    }

    /// Create an unauthorized error.
    pub fn unauthorized(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::Unauthorized,
            message: msg.to_string(),
        }
    }

    /// Create a forbidden error.
    pub fn forbidden(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::Forbidden,
            message: msg.to_string(),
        }
    }

    /// Create an unprocessable entity error.
    pub fn unprocessable_entity(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::UnprocessableEntity,
            message: msg.to_string(),
        }
    }

    /// Create a too early error.
    pub fn too_early(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::TooEarly,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn internal_server_error(msg: &str) -> HttpError {
        HttpError {
            status: HttpStatus::InternalServerError,
            message: msg.to_string(),
        }
    }
}
