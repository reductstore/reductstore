// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use int_enum::IntEnum;
use std::error::Error;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::time::SystemTimeError;
use url::ParseError;

/// HTTP status codes + client errors (negative).
#[repr(i16)]
#[derive(Debug, PartialEq, PartialOrd, Copy, Clone, IntEnum)]
pub enum ErrorCode {
    UrlParseError = -4,
    ConnectionError = -3,
    Timeout = -2,
    Unknown = -1,

    Continue = 100,
    OK = 200,
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
#[derive(PartialEq, Debug, Clone)]
pub struct ReductError {
    /// The HTTP status code.
    pub status: ErrorCode,

    /// The human readable message.
    pub message: String,
}

impl Display for ReductError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "[{:?}] {}", self.status, self.message)
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{}", self.int_value())
    }
}

impl From<std::io::Error> for ReductError {
    fn from(err: std::io::Error) -> Self {
        // An IO error is an internal reductstore error
        ReductError {
            status: ErrorCode::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl From<SystemTimeError> for ReductError {
    fn from(err: SystemTimeError) -> Self {
        // A system time error is an internal reductstore error
        ReductError {
            status: ErrorCode::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl From<ParseError> for ReductError {
    fn from(err: ParseError) -> Self {
        // A parse error is an internal reductstore error
        ReductError {
            status: ErrorCode::UrlParseError,
            message: err.to_string(),
        }
    }
}

impl Error for ReductError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl ReductError {
    pub fn new(status: ErrorCode, message: &str) -> Self {
        ReductError {
            status,
            message: message.to_string(),
        }
    }

    pub fn status(&self) -> ErrorCode {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn ok() -> ReductError {
        ReductError {
            status: ErrorCode::OK,
            message: "".to_string(),
        }
    }

    /// Create a no content error.
    pub fn no_content(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::NoContent,
            message: msg.to_string(),
        }
    }

    /// Create a not found error.
    pub fn not_found(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::NotFound,
            message: msg.to_string(),
        }
    }

    /// Create a conflict error.
    pub fn conflict(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::Conflict,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn bad_request(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::BadRequest,
            message: msg.to_string(),
        }
    }

    /// Create an unauthorized error.
    pub fn unauthorized(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::Unauthorized,
            message: msg.to_string(),
        }
    }

    /// Create a forbidden error.
    pub fn forbidden(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::Forbidden,
            message: msg.to_string(),
        }
    }

    /// Create an unprocessable entity error.
    pub fn unprocessable_entity(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::UnprocessableEntity,
            message: msg.to_string(),
        }
    }

    /// Create a too early error.
    pub fn too_early(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::TooEarly,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn internal_server_error(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::InternalServerError,
            message: msg.to_string(),
        }
    }
}

#[macro_export]
macro_rules! too_early {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::too_early(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::too_early($msg)
    };
}

#[macro_export]
macro_rules! internal_server_error {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::internal_server_error(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::internal_server_error($msg)
    };
}
