// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::{Components, StateKeeper};
use crate::api::http::HttpError;
use crate::audit::AuditEvent;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use log::{debug, error, Level};
use reduct_base::error::ErrorCode;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

pub(super) async fn audit_requests(
    State(keeper): State<Arc<StateKeeper>>,
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, HttpError> {
    let start = Instant::now();

    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());

    let response = next.run(request).await;

    if should_skip_audit(&path) {
        return Ok(response);
    }

    let components = get_audit_components(&keeper).await;
    let token_name = resolve_audit_token_name(
        response.status(),
        auth_header.as_deref(),
        components.as_ref(),
    )
    .await;

    if let (Some(token_name), Some(components)) = (token_name, components) {
        write_audit_event(
            &components,
            token_name,
            format!("{} {}", method, path),
            response.status().as_u16(),
            start.elapsed().as_micros() as u64,
        )
        .await;
    }

    Ok(response)
}

fn should_skip_audit(path: &str) -> bool {
    // Maybe skip /alive and /ready and /audit endpoints
    path.ends_with("/alive") || path.ends_with("/ready") || path.contains("/audit")
}

async fn get_audit_components(keeper: &StateKeeper) -> Option<Arc<Components>> {
    match keeper.get_anonymous().await {
        Ok(components) => Some(components),
        Err(err) => {
            debug!("Failed to get components for audit: {}", err);
            None
        }
    }
}

async fn resolve_audit_token_name(
    status: StatusCode,
    auth_header: Option<&str>,
    components: Option<&Arc<Components>>,
) -> Option<String> {
    if status == StatusCode::UNAUTHORIZED {
        Some("unauthorized".to_string())
    } else if let (Some(header), Some(components)) = (auth_header, components) {
        match components.token_repo.write().await {
            Ok(mut token_repo) => match token_repo.validate_token(Some(header)).await {
                Ok(token) => Some(token.name),
                Err(err) => {
                    debug!("Failed to validate token for audit: {}", err);
                    None
                }
            },
            Err(err) => {
                debug!("Failed to lock token repository for audit: {}", err);
                None
            }
        }
    } else {
        None
    }
}

async fn write_audit_event(
    components: &Arc<Components>,
    token_name: String,
    endpoint: String,
    status: u16,
    duration: u64,
) {
    let event = AuditEvent {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64,
        token_name,
        endpoint,
        status,
        call_count: 1,
        duration,
    };

    match components.audit_repo.write().await {
        Ok(mut audit_repo) => {
            if let Err(err) = audit_repo.log_event(event).await {
                debug!("Failed to persist audit event: {}", err);
            }
        }
        Err(err) => debug!("Failed to lock audit repository: {}", err),
    }
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
