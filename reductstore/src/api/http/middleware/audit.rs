// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::{Components, StateKeeper};
use crate::api::http::middleware::client_ip::client_ip_from_request;
use crate::api::http::HttpError;
use crate::audit::AuditEvent;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use log::debug;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    let client_ip = client_ip_from_request(&request);

    let response = next.run(request).await;

    if should_skip_audit(&path) {
        return Ok(response);
    }

    let components = get_audit_components(&keeper).await;
    let token_name = resolve_audit_token_name(
        response.status(),
        auth_header.as_deref(),
        client_ip,
        components.as_ref(),
    )
    .await;

    if let (Some(token_name), Some(components)) = (token_name, components) {
        let message = response
            .headers()
            .get("x-reduct-error")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();

        write_audit_event(
            &components,
            token_name,
            format!("{} {}", method, path),
            response.status().as_u16(),
            message,
            client_ip.map(|ip| ip.to_string()),
            start.elapsed().as_secs_f64(),
        )
        .await;
    }

    Ok(response)
}

pub(super) fn should_skip_audit(path: &str) -> bool {
    path.ends_with("/alive") || path.ends_with("/ready")
}

pub(super) async fn get_audit_components(keeper: &StateKeeper) -> Option<Arc<Components>> {
    match keeper.get_anonymous().await {
        Ok(components) => Some(components),
        Err(err) => {
            debug!("Failed to get components for audit: {}", err);
            None
        }
    }
}

pub(super) async fn resolve_audit_token_name(
    status: StatusCode,
    auth_header: Option<&str>,
    client_ip: Option<IpAddr>,
    components: Option<&Arc<Components>>,
) -> Option<String> {
    if status == StatusCode::UNAUTHORIZED {
        Some("unauthorized".to_string())
    } else if let (Some(header), Some(components)) = (auth_header, components) {
        match components.token_repo.write().await {
            Ok(mut token_repo) => match token_repo.validate_token(Some(header), client_ip).await {
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
    message: String,
    client_ip: Option<String>,
    duration: f64,
) {
    let event = AuditEvent {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64,
        instance: components.cfg.instance_name.clone(),
        token_name,
        endpoint,
        status,
        message,
        client_ip,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{keeper, waiting_keeper};
    use rstest::rstest;

    #[test]
    fn should_skip_only_internal_health_paths() {
        assert!(should_skip_audit("/api/v1/alive"));
        assert!(should_skip_audit("/api/v1/ready"));
        assert!(!should_skip_audit("/api/v1/info"));
    }

    #[tokio::test]
    async fn resolve_audit_token_name_returns_unauthorized_for_401() {
        let result = resolve_audit_token_name(StatusCode::UNAUTHORIZED, None, None, None).await;
        assert_eq!(result, Some("unauthorized".to_string()));
    }

    #[tokio::test]
    async fn resolve_audit_token_name_returns_none_for_missing_components() {
        let result =
            resolve_audit_token_name(StatusCode::OK, Some("Bearer init-token"), None, None).await;
        assert_eq!(result, None);
    }

    #[rstest]
    #[tokio::test]
    async fn get_audit_components_returns_some(#[future] keeper: Arc<StateKeeper>) {
        let keeper = keeper.await;
        assert!(get_audit_components(&keeper).await.is_some());
    }

    #[rstest]
    #[tokio::test]
    async fn get_audit_components_returns_none_when_unavailable(
        #[future] waiting_keeper: Arc<StateKeeper>,
    ) {
        let keeper = waiting_keeper.await;
        assert!(get_audit_components(&keeper).await.is_none());
    }
}
