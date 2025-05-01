// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use int_enum::IntEnum;
use std::error::Error;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::sync::PoisonError;
use std::time::SystemTimeError;
use url::ParseError;

#[cfg(feature = "io")]
use tokio::sync::mpsc::error::SendError;

/// HTTP status codes + client errors (negative).
#[repr(i16)]
#[derive(Debug, PartialEq, PartialOrd, Copy, Clone, IntEnum)]
pub enum ErrorCode {
    InvalidRequest = -6, // used for invalid requests
    Interrupt = -5,      // used for interrupting a long-running task or query
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

impl<T> From<PoisonError<T>> for ReductError {
    fn from(_: PoisonError<T>) -> Self {
        // A poison error is an internal reductstore error
        ReductError {
            status: ErrorCode::InternalServerError,
            message: "Poison error".to_string(),
        }
    }
}

impl From<Box<dyn std::any::Any + Send>> for ReductError {
    fn from(err: Box<dyn std::any::Any + Send>) -> Self {
        // A box error is an internal reductstore error
        ReductError {
            status: ErrorCode::InternalServerError,
            message: format!("{:?}", err),
        }
    }
}

#[cfg(feature = "io")]
impl<T> From<SendError<T>> for ReductError {
    fn from(err: SendError<T>) -> Self {
        // A send error is an internal reductstore error
        ReductError {
            status: ErrorCode::InternalServerError,
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

    pub fn timeout(msg: &str) -> ReductError {
        ReductError {
            status: ErrorCode::Timeout,
            message: msg.to_string(),
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

// Macros for creating errors with a message.

#[macro_export]
macro_rules! timeout {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::timeout(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::timeout($msg)
    };
}

#[macro_export]
macro_rules! no_content {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::no_content(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::no_content($msg)
    };
}

#[macro_export]
macro_rules! bad_request {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::bad_request(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::bad_request($msg)
    };
}

#[macro_export]
macro_rules! unprocessable_entity {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::unprocessable_entity(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::unprocessable_entity($msg)
    };
}

#[macro_export]
macro_rules! not_found {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::not_found(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::not_found($msg)
    };
}
#[macro_export]
macro_rules! conflict {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::conflict(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::conflict($msg)
    };
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

#[macro_export]
macro_rules! unauthorized {
    ($msg:expr, $($arg:tt)*) => {
        ReductError::unauthorized(&format!($msg, $($arg)*))
    };
    ($msg:expr) => {
        ReductError::unauthorized($msg)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn creates_internal_server_error() {
        let error = ReductError::internal_server_error("Unexpected server error");
        assert_eq!(error.status, ErrorCode::InternalServerError);
        assert_eq!(error.message, "Unexpected server error");
    }

    #[test]
    fn converts_io_error_to_reduct_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::Other, "IO failure");
        let error: ReductError = io_error.into();
        assert_eq!(error.status, ErrorCode::InternalServerError);
        assert_eq!(error.message, "IO failure");
    }

    #[test]
    fn converts_system_time_error_to_reduct_error() {
        let system_time_error = UNIX_EPOCH.duration_since(SystemTime::now()).unwrap_err();
        let error: ReductError = system_time_error.into();
        assert_eq!(error.status, ErrorCode::InternalServerError);
        assert_eq!(error.message, "second time provided was later than self");
    }

    #[test]
    fn converts_url_parse_error_to_reduct_error() {
        let parse_error = ParseError::EmptyHost;
        let error: ReductError = parse_error.into();
        assert_eq!(error.status, ErrorCode::UrlParseError);
        assert_eq!(error.message, "empty host");
    }

    #[test]
    fn converts_poison_error_to_reduct_error() {
        let poison_error: PoisonError<()> = PoisonError::new(());
        let error: ReductError = poison_error.into();
        assert_eq!(error.status, ErrorCode::InternalServerError);
        assert_eq!(error.message, "Poison error");
    }

    #[cfg(feature = "io")]
    #[test]
    fn converts_send_error_to_reduct_error() {
        let send_error: SendError<()> = SendError(());
        let error: ReductError = send_error.into();
        assert_eq!(error.status, ErrorCode::InternalServerError);
        assert_eq!(error.message, "SendError");
    }

    mod macros {
        use super::*;

        #[test]
        fn test_timeout_macro() {
            let error = timeout!("Timeout error: {}", 42);
            assert_eq!(error.status, ErrorCode::Timeout);
            assert_eq!(error.message, "Timeout error: 42");
        }

        #[test]
        fn test_no_content_macro() {
            let error = no_content!("No content error: {}", 42);
            assert_eq!(error.status, ErrorCode::NoContent);
            assert_eq!(error.message, "No content error: 42");
        }

        #[test]
        fn test_bad_request_macro() {
            let error = bad_request!("Bad request error: {}", 42);
            assert_eq!(error.status, ErrorCode::BadRequest);
            assert_eq!(error.message, "Bad request error: 42");
        }

        #[test]
        fn test_unprocessable_entity_macro() {
            let error = unprocessable_entity!("Unprocessable entity error: {}", 42);
            assert_eq!(error.status, ErrorCode::UnprocessableEntity);
            assert_eq!(error.message, "Unprocessable entity error: 42");
        }

        #[test]
        fn test_not_found_macro() {
            let error = not_found!("Not found error: {}", 42);
            assert_eq!(error.status, ErrorCode::NotFound);
            assert_eq!(error.message, "Not found error: 42");
        }

        #[test]
        fn test_conflict_macro() {
            let error = conflict!("Conflict error: {}", 42);
            assert_eq!(error.status, ErrorCode::Conflict);
            assert_eq!(error.message, "Conflict error: 42");
        }

        #[test]
        fn test_too_early_macro() {
            let error = too_early!("Too early error: {}", 42);
            assert_eq!(error.status, ErrorCode::TooEarly);
            assert_eq!(error.message, "Too early error: 42");
        }

        #[test]
        fn test_internal_server_error_macro() {
            let error = internal_server_error!("Internal server error: {}", 42);
            assert_eq!(error.status, ErrorCode::InternalServerError);
            assert_eq!(error.message, "Internal server error: 42");
        }
    }
}
