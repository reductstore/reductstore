// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::StateKeeper;
use axum::body::Body;
use axum::http::{Request, StatusCode};

use axum::extract::State;
use axum::middleware::Next;
use axum::response::IntoResponse;
use log::{debug, error, Level};
use reduct_base::error::ErrorCode;

use crate::api::http::HttpError;
use std::sync::Arc;

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

pub(super) async fn check_api_rate_limit(
    State(keeper): State<Arc<StateKeeper>>,
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, HttpError> {
    let components = keeper.get_anonymous().await?;
    if let Err(err) = components.limits.check_api_request().await {
        return Err(HttpError::from(err));
    }

    Ok(next.run(request).await)
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

    match log_level_for_response(response.status(), skip_error_log) {
        Level::Error => error!(
            "{} [{}] {}, {:?}",
            method,
            response.status(),
            err_msg,
            strat_time.elapsed()
        ),
        _ => debug!(
            "{} [{}] {}, {:?}",
            method,
            response.status(),
            err_msg,
            strat_time.elapsed()
        ),
    }

    Ok(response)
}

fn log_level_for_response(status: StatusCode, skip_error_log: bool) -> Level {
    if status.is_server_error() && !skip_error_log {
        Level::Error
    } else {
        Level::Debug
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::components::StateKeeper;
    use crate::api::http::tests::api_limited_keeper;
    use axum::http::Request;
    use axum::http::{HeaderMap, HeaderValue};
    use axum::routing::get;
    use axum::{middleware::from_fn_with_state, Router};
    use log::Level;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[test_log::test]
    fn selects_error_for_server_error_without_hint() {
        let level = log_level_for_response(StatusCode::INTERNAL_SERVER_ERROR, false);
        assert_eq!(level, Level::Error);
    }

    #[test_log::test]
    fn respects_skip_hint_even_for_server_errors() {
        let level = log_level_for_response(StatusCode::INTERNAL_SERVER_ERROR, true);
        assert_eq!(level, Level::Debug);
    }

    #[test_log::test]
    fn uses_debug_for_non_server_errors() {
        let level = log_level_for_response(StatusCode::BAD_REQUEST, false);
        assert_eq!(level, Level::Debug);
    }

    #[test_log::test]
    fn detects_skip_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-reduct-log-hint", HeaderValue::from_static("keep"));
        assert!(!headers
            .get("x-reduct-log-hint")
            .is_some_and(|v| v == "skip-error-log"));
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn enforces_api_rate_limit(#[future] api_limited_keeper: Arc<StateKeeper>) {
        let keeper = api_limited_keeper.await;
        let app = Router::new()
            .route("/test", get(|| async { StatusCode::OK }))
            .layer(from_fn_with_state(
                Arc::clone(&keeper),
                check_api_rate_limit,
            ));

        let first = app
            .clone()
            .oneshot(Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(second
            .headers()
            .get("x-reduct-error")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("api requests"));
    }
}
