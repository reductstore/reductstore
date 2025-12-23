// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use axum::body::Body;
use axum::http::Request;

use axum::middleware::Next;
use axum::response::IntoResponse;
use log::{debug, error};
use reduct_base::error::ErrorCode;

use crate::api::HttpError;

pub(super) async fn default_headers(
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, HttpError> {
    let mut response = next.run(request).await;
    let version: &str = env!("CARGO_PKG_VERSION");
    response.headers_mut().insert(
        "Server",
        format!("ReductStore {}", version).parse().unwrap(),
    );

    let tokens: Vec<&str> = version.splitn(3, ".").collect();
    response.headers_mut().insert(
        "x-reduct-api",
        format!("{}.{}", tokens[0], tokens[1]).parse().unwrap(),
    );
    Ok(response)
}

pub async fn print_statuses(
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, HttpError> {
    if request.uri().path_and_query().is_none() {
        return Err(HttpError::new(
            ErrorCode::BadRequest,
            "Failed to get path and query",
        ));
    }

    let strat_time = std::time::Instant::now();
    let path_and_query = request.uri().path_and_query().unwrap();
    let method = format!("{} {}", request.method(), path_and_query);

    let response = next.run(request).await;
    let err_msg = match response.headers().get("x-reduct-error") {
        Some(msg) => msg.to_str().unwrap_or("Failed to get error message"),
        None => "",
    };

    let skip_error_log = response
        .headers()
        .get("x-reduct-log-hint")
        .is_some_and(|v| v == "skip-error-log");

    if response.status().is_server_error() && !skip_error_log {
        error!(
            "{} [{}] {}, {:?}",
            method,
            response.status(),
            err_msg,
            strat_time.elapsed()
        );
    } else {
        debug!(
            "{} [{}] {}, {:?}",
            method,
            response.status(),
            err_msg,
            strat_time.elapsed()
        );
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;
    use axum::http::HeaderValue;

    #[test]
    fn detects_skip_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-reduct-log-hint",
            HeaderValue::from_static("skip-error-log"),
        );
        assert!(headers
            .get("x-reduct-log-hint")
            .is_some_and(|v| v == "skip-error-log"));

        let mut headers = HeaderMap::new();
        headers.insert("x-reduct-log-hint", HeaderValue::from_static("keep"));
        assert!(!headers
            .get("x-reduct-log-hint")
            .is_some_and(|v| v == "skip-error-log"));
    }
}
