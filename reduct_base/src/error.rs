// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use int_enum::IntEnum;
use std::error::Error;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::time::SystemTimeError;

/// HTTP status codes + communication errors.
#[repr(i16)]
#[derive(Debug, PartialEq, PartialOrd, Copy, Clone, IntEnum)]
pub enum ErrorCode {
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
#[derive(PartialEq, Debug)]
pub struct HttpError {
    /// The HTTP status code.
    pub status: ErrorCode,

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
            status: ErrorCode::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl From<SystemTimeError> for HttpError {
    fn from(err: SystemTimeError) -> Self {
        // A system time error is an internal reductstore error
        HttpError {
            status: ErrorCode::InternalServerError,
            message: err.to_string(),
        }
    }
}

impl Error for HttpError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl HttpError {
    pub fn new(status: ErrorCode, message: &str) -> Self {
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

    pub fn to_string(&self) -> String {
        format!("[{:?}] {}", self.status, self.message)
    }

    pub fn ok() -> HttpError {
        HttpError {
            status: ErrorCode::OK,
            message: "".to_string(),
        }
    }

    /// Create a no content error.
    pub fn no_content(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::NoContent,
            message: msg.to_string(),
        }
    }

    /// Create a not found error.
    pub fn not_found(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::NotFound,
            message: msg.to_string(),
        }
    }

    /// Create a conflict error.
    pub fn conflict(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::Conflict,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn bad_request(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::BadRequest,
            message: msg.to_string(),
        }
    }

    /// Create an unauthorized error.
    pub fn unauthorized(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::Unauthorized,
            message: msg.to_string(),
        }
    }

    /// Create a forbidden error.
    pub fn forbidden(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::Forbidden,
            message: msg.to_string(),
        }
    }

    /// Create an unprocessable entity error.
    pub fn unprocessable_entity(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::UnprocessableEntity,
            message: msg.to_string(),
        }
    }

    /// Create a too early error.
    pub fn too_early(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::TooEarly,
            message: msg.to_string(),
        }
    }

    /// Create a bad request error.
    pub fn internal_server_error(msg: &str) -> HttpError {
        HttpError {
            status: ErrorCode::InternalServerError,
            message: msg.to_string(),
        }
    }
}
