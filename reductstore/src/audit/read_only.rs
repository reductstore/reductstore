// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::aggregator::{AuditAggregator, FlushHandler};
use crate::audit::{AuditEvent, ManageAudit, AUDIT_BUCKET_NAME};
use crate::cfg::Cfg;
use crate::core::sync::AsyncRwLock;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::internal_server_error;
use reduct_base::unprocessable_entity;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Client, Response};
use std::sync::Arc;
use url::form_urlencoded;

pub(crate) struct ReadOnlyAuditRepository {
    aggregator: AuditAggregator,
}

impl ReadOnlyAuditRepository {
    pub async fn new(cfg: Cfg, _storage: Arc<StorageEngine>) -> Self {
        let client = Self::build_client(&cfg).expect("audit replica client must build");
        let primary_url = normalize_url(cfg.primary_url.clone());
        let secondary_url = normalize_url(cfg.secondary_url.clone());
        let preferred_url = Arc::new(AsyncRwLock::new(None));
        let instance_name = cfg.instance_name.clone();

        let handler: FlushHandler = Arc::new(move |event| {
            let client = client.clone();
            let primary_url = primary_url.clone();
            let secondary_url = secondary_url.clone();
            let preferred_url = Arc::clone(&preferred_url);
            let instance_name = instance_name.clone();

            Box::pin(async move {
                Self::log_event_with_failover(
                    &client,
                    primary_url.as_deref(),
                    secondary_url.as_deref(),
                    preferred_url,
                    &instance_name,
                    &event,
                )
                .await
            })
        });

        let aggregator = AuditAggregator::new(handler);
        Self { aggregator }
    }

    fn build_client(cfg: &Cfg) -> Result<Client, ReductError> {
        let mut headers = HeaderMap::new();
        if !cfg.api_token.is_empty() {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", cfg.api_token)).unwrap();
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }

        let mut builder = Client::builder()
            .default_headers(headers)
            .connect_timeout(cfg.audit_conf.remote_timeout)
            .danger_accept_invalid_certs(!cfg.audit_conf.remote_verify_ssl)
            .http1_only();

        if let Some(path) = &cfg.audit_conf.remote_ca_path {
            let cert_data = std::fs::read(path).map_err(|err| {
                internal_server_error!(
                    "Failed to read audit remote CA certificate {}: {}",
                    path.display(),
                    err
                )
            })?;
            let cert = reqwest::Certificate::from_pem(&cert_data).map_err(|err| {
                internal_server_error!(
                    "Failed to parse audit remote CA certificate {}: {}",
                    path.display(),
                    err
                )
            })?;
            builder = builder.add_root_certificate(cert);
        }

        builder.build().map_err(|err| {
            internal_server_error!("Failed to build audit replica HTTP client: {}", err)
        })
    }

    async fn log_event_with_failover(
        client: &Client,
        primary_url: Option<&str>,
        secondary_url: Option<&str>,
        preferred_url: Arc<AsyncRwLock<Option<String>>>,
        instance_name: &str,
        event: &AuditEvent,
    ) -> Result<(), ReductError> {
        let mut candidates = Vec::new();

        if let Some(url) = preferred_url.read().await?.clone() {
            candidates.push(url);
        }
        if let Some(url) = primary_url {
            if !candidates.iter().any(|candidate| candidate == url) {
                candidates.push(url.to_string());
            }
        }
        if let Some(url) = secondary_url {
            if !candidates.iter().any(|candidate| candidate == url) {
                candidates.push(url.to_string());
            }
        }

        if candidates.is_empty() {
            return Err(unprocessable_entity!(
                "Neither primary nor secondary URL is configured for replica audit writes"
            ));
        }

        let mut last_err = None;
        for base_url in candidates {
            match Self::log_event_to_url(client, &base_url, instance_name, event).await {
                Ok(()) => {
                    *preferred_url.write().await? = Some(base_url);
                    return Ok(());
                }
                Err(err) => {
                    if !is_failover_candidate(&err) {
                        return Err(err);
                    }
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            unprocessable_entity!(
                "Neither primary nor secondary URL is configured for replica audit writes"
            )
        }))
    }

    async fn log_event_to_url(
        client: &Client,
        base_url: &str,
        instance_name: &str,
        event: &AuditEvent,
    ) -> Result<(), ReductError> {
        let url = build_write_url(base_url, event);
        let payload = serde_json::to_vec(event)
            .map_err(|err| internal_server_error!("Failed to serialize audit event: {}", err))?;

        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&payload.len().to_string()).unwrap(),
        );
        headers.insert(
            "x-reduct-label-status",
            HeaderValue::from_str(&event.status.to_string()).map_err(|err| {
                unprocessable_entity!("Invalid audit status label '{}': {}", event.status, err)
            })?,
        );
        headers.insert(
            "x-reduct-label-instance",
            HeaderValue::from_str(instance_name).map_err(|err| {
                unprocessable_entity!("Invalid audit instance label '{}': {}", instance_name, err)
            })?,
        );

        let response = client
            .post(url)
            .headers(headers)
            .body(Body::from(Bytes::from(payload)))
            .send()
            .await;

        check_response(response)?;
        Ok(())
    }
}

fn normalize_url(url: Option<String>) -> Option<String> {
    url.map(|url| {
        if url.ends_with('/') {
            url
        } else {
            format!("{}/", url)
        }
    })
}

fn build_write_url(base_url: &str, event: &AuditEvent) -> String {
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("ts", &event.timestamp.to_string());
    let query = query.finish();
    format!(
        "{}api/v1/b/{}/{}?{}",
        base_url, AUDIT_BUCKET_NAME, event.token_name, query
    )
}

fn is_failover_candidate(err: &ReductError) -> bool {
    matches!(err.status, ErrorCode::ConnectionError | ErrorCode::Timeout)
}

fn check_response(response: Result<Response, reqwest::Error>) -> Result<Response, ReductError> {
    let map_error = |error: reqwest::Error| -> ReductError {
        let status = if error.is_connect() {
            ErrorCode::ConnectionError
        } else if error.is_timeout() {
            ErrorCode::Timeout
        } else if error.is_request() {
            ErrorCode::InvalidRequest
        } else {
            ErrorCode::Unknown
        };

        ReductError::new(status, &error.to_string())
    };

    let response = response.map_err(map_error)?;
    if response.status().is_success() {
        return Ok(response);
    }

    let status =
        ErrorCode::try_from(response.status().as_u16() as i16).unwrap_or(ErrorCode::Unknown);

    let error_msg = response
        .headers()
        .get("x-reduct-error")
        .unwrap_or(&HeaderValue::from_str("Unknown").unwrap())
        .to_str()
        .unwrap()
        .to_string();

    Err(ReductError::new(status, &error_msg))
}

#[async_trait]
impl ManageAudit for ReadOnlyAuditRepository {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError> {
        self.aggregator.log_event(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::aggregator::AGGREGATION_WINDOW_SECS;
    use crate::cfg::Cfg;
    use crate::storage::engine::StorageEngine;
    use axum::body::Bytes as AxumBytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::any;
    use axum::Router;
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};

    #[derive(Clone)]
    struct TestServerState {
        status: StatusCode,
        error_header: Option<&'static str>,
        events: Arc<Mutex<Vec<AuditEvent>>>,
        auth_headers: Arc<Mutex<Vec<Option<String>>>>,
        status_labels: Arc<Mutex<Vec<Option<String>>>>,
        instance_labels: Arc<Mutex<Vec<Option<String>>>>,
    }

    async fn audit_handler(
        State(state): State<TestServerState>,
        headers: axum::http::HeaderMap,
        body: AxumBytes,
    ) -> impl IntoResponse {
        let event: AuditEvent = serde_json::from_slice(&body).unwrap();
        state.events.lock().await.push(event);
        state.auth_headers.lock().await.push(
            headers
                .get(reqwest::header::AUTHORIZATION.as_str())
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string()),
        );
        state.status_labels.lock().await.push(
            headers
                .get("x-reduct-label-status")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string()),
        );
        state.instance_labels.lock().await.push(
            headers
                .get("x-reduct-label-instance")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string()),
        );

        let mut response = state.status.into_response();
        if let Some(message) = state.error_header {
            response
                .headers_mut()
                .insert("x-reduct-error", HeaderValue::from_static(message));
        }

        response
    }

    async fn start_test_server(
        status: StatusCode,
        error_header: Option<&'static str>,
    ) -> (
        String,
        Arc<Mutex<Vec<AuditEvent>>>,
        Arc<Mutex<Vec<Option<String>>>>,
        Arc<Mutex<Vec<Option<String>>>>,
        Arc<Mutex<Vec<Option<String>>>>,
    ) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let auth_headers = Arc::new(Mutex::new(Vec::new()));
        let status_labels = Arc::new(Mutex::new(Vec::new()));
        let instance_labels = Arc::new(Mutex::new(Vec::new()));
        let state = TestServerState {
            status,
            error_header,
            events: Arc::clone(&events),
            auth_headers: Arc::clone(&auth_headers),
            status_labels: Arc::clone(&status_labels),
            instance_labels: Arc::clone(&instance_labels),
        };

        let app = Router::new()
            .route("/{*path}", any(audit_handler))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (
            format!("http://{}/", addr),
            events,
            auth_headers,
            status_labels,
            instance_labels,
        )
    }

    #[fixture]
    async fn storage() -> Arc<StorageEngine> {
        let tmp_dir = tempdir().unwrap();
        let cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        )
    }

    fn make_cfg(primary_url: Option<String>, secondary_url: Option<String>) -> Cfg {
        Cfg {
            api_token: "admin-token".to_string(),
            primary_url,
            secondary_url,
            ..Cfg::default()
        }
    }

    fn make_event(timestamp: u64) -> AuditEvent {
        AuditEvent {
            timestamp,
            token_name: "token-1".to_string(),
            endpoint: "GET /api/v1/info".to_string(),
            status: 200,
            call_count: 1,
            duration: 100,
        }
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn uses_primary_url_and_remembers_it() {
        let (primary_url, primary_events, _, _, _) = start_test_server(StatusCode::OK, None).await;
        let preferred_url = Arc::new(AsyncRwLock::new(None));
        let cfg = make_cfg(Some(primary_url.clone()), None);
        let client = ReadOnlyAuditRepository::build_client(&cfg).unwrap();

        ReadOnlyAuditRepository::log_event_with_failover(
            &client,
            Some(&primary_url),
            None,
            Arc::clone(&preferred_url),
            "instance-a",
            &make_event(1),
        )
        .await
        .unwrap();

        assert_eq!(*preferred_url.read().await.unwrap(), Some(primary_url));
        assert_eq!(primary_events.lock().await.len(), 1);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn falls_back_to_secondary_on_connection_error() {
        let (secondary_url, secondary_events, _, _, _) =
            start_test_server(StatusCode::OK, None).await;
        let preferred_url = Arc::new(AsyncRwLock::new(None));
        let cfg = make_cfg(
            Some("http://127.0.0.1:1/".to_string()),
            Some(secondary_url.clone()),
        );
        let client = ReadOnlyAuditRepository::build_client(&cfg).unwrap();

        ReadOnlyAuditRepository::log_event_with_failover(
            &client,
            Some("http://127.0.0.1:1/"),
            Some(&secondary_url),
            Arc::clone(&preferred_url),
            "instance-a",
            &make_event(1),
        )
        .await
        .unwrap();

        assert_eq!(*preferred_url.read().await.unwrap(), Some(secondary_url));
        assert_eq!(secondary_events.lock().await.len(), 1);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn prefers_last_successful_url_before_primary() {
        let (primary_url, primary_events, _, _, _) = start_test_server(StatusCode::OK, None).await;
        let (secondary_url, secondary_events, _, _, _) =
            start_test_server(StatusCode::OK, None).await;
        let preferred_url = Arc::new(AsyncRwLock::new(Some(secondary_url.clone())));
        let cfg = make_cfg(Some(primary_url.clone()), Some(secondary_url.clone()));
        let client = ReadOnlyAuditRepository::build_client(&cfg).unwrap();

        ReadOnlyAuditRepository::log_event_with_failover(
            &client,
            Some(&primary_url),
            Some(&secondary_url),
            Arc::clone(&preferred_url),
            "instance-a",
            &make_event(1),
        )
        .await
        .unwrap();

        assert_eq!(primary_events.lock().await.len(), 0);
        assert_eq!(secondary_events.lock().await.len(), 1);
        assert_eq!(*preferred_url.read().await.unwrap(), Some(secondary_url));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn does_not_fail_over_on_http_error() {
        let (primary_url, primary_events, _, _, _) =
            start_test_server(StatusCode::FORBIDDEN, Some("denied")).await;
        let (secondary_url, secondary_events, _, _, _) =
            start_test_server(StatusCode::OK, None).await;
        let preferred_url = Arc::new(AsyncRwLock::new(None));
        let cfg = make_cfg(Some(primary_url.clone()), Some(secondary_url.clone()));
        let client = ReadOnlyAuditRepository::build_client(&cfg).unwrap();

        let err = ReadOnlyAuditRepository::log_event_with_failover(
            &client,
            Some(&primary_url),
            Some(&secondary_url),
            Arc::clone(&preferred_url),
            "instance-a",
            &make_event(1),
        )
        .await
        .unwrap_err();

        assert_eq!(err.status, ErrorCode::Forbidden);
        assert_eq!(primary_events.lock().await.len(), 1);
        assert_eq!(secondary_events.lock().await.len(), 0);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn fails_when_no_replica_urls_are_configured() {
        let preferred_url = Arc::new(AsyncRwLock::new(None));
        let cfg = make_cfg(None, None);
        let client = ReadOnlyAuditRepository::build_client(&cfg).unwrap();

        let err = ReadOnlyAuditRepository::log_event_with_failover(
            &client,
            None,
            None,
            Arc::clone(&preferred_url),
            "instance-a",
            &make_event(1),
        )
        .await
        .unwrap_err();

        assert_eq!(err.status, ErrorCode::UnprocessableEntity);
        assert_eq!(
            err.message,
            "Neither primary nor secondary URL is configured for replica audit writes"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn aggregates_before_forwarding(#[future] storage: Arc<StorageEngine>) {
        let (primary_url, primary_events, _, _, _) = start_test_server(StatusCode::OK, None).await;
        let storage = storage.await;
        let cfg = make_cfg(Some(primary_url), None);
        let mut repo = ReadOnlyAuditRepository::new(cfg, storage).await;

        repo.log_event(make_event(1)).await.unwrap();
        repo.log_event(make_event(2)).await.unwrap();

        sleep(Duration::from_millis((AGGREGATION_WINDOW_SECS * 1000) / 2)).await;
        assert_eq!(primary_events.lock().await.len(), 0);

        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;
        let events = primary_events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].call_count, 2);
        assert_eq!(events[0].duration, 200);
        assert_eq!(events[0].timestamp, 1);
    }

    #[rstest]
    #[case(Some("https://example.com/".to_string()), Some("https://example.com/".to_string()))]
    #[case(Some("https://example.com".to_string()), Some("https://example.com/".to_string()))]
    #[case(None, None)]
    fn normalizes_replica_urls(#[case] url: Option<String>, #[case] expected: Option<String>) {
        assert_eq!(normalize_url(url), expected);
    }

    #[test]
    fn builds_write_url_for_audit_bucket() {
        let event = make_event(42);
        assert_eq!(
            build_write_url("https://primary.example.com/", &event),
            "https://primary.example.com/api/v1/b/$audit/token-1?ts=42"
        );
    }

    #[rstest]
    #[case("admin-token", Some("Bearer admin-token".to_string()))]
    #[case("", None)]
    #[tokio::test(flavor = "multi_thread")]
    async fn forwards_expected_headers(
        #[case] api_token: &str,
        #[case] expected_auth_header: Option<String>,
    ) {
        let (base_url, events, auth_headers, status_labels, instance_labels) =
            start_test_server(StatusCode::OK, None).await;
        let cfg = Cfg {
            api_token: api_token.to_string(),
            ..Cfg::default()
        };
        let client = ReadOnlyAuditRepository::build_client(&cfg).unwrap();

        ReadOnlyAuditRepository::log_event_to_url(&client, &base_url, "instance-a", &make_event(1))
            .await
            .unwrap();

        assert_eq!(events.lock().await.len(), 1);
        assert_eq!(auth_headers.lock().await[0], expected_auth_header);
        assert_eq!(status_labels.lock().await[0], Some("200".to_string()));
        assert_eq!(
            instance_labels.lock().await[0],
            Some("instance-a".to_string())
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn uses_unknown_error_message_when_header_is_missing() {
        let (base_url, _, _, _, _) = start_test_server(StatusCode::FORBIDDEN, None).await;
        let client = ReadOnlyAuditRepository::build_client(&Cfg::default()).unwrap();

        let err = ReadOnlyAuditRepository::log_event_to_url(
            &client,
            &base_url,
            "instance-a",
            &make_event(1),
        )
        .await
        .unwrap_err();

        assert_eq!(err.status, ErrorCode::Forbidden);
        assert_eq!(err.message, "Unknown");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn build_client_accepts_valid_custom_ca_path() {
        let mut cfg = Cfg::default();
        let cert_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("misc")
            .join("certificate.crt");
        cfg.audit_conf.remote_ca_path = Some(cert_path);

        assert!(ReadOnlyAuditRepository::build_client(&cfg).is_ok());
    }

    #[test]
    fn build_client_rejects_missing_custom_ca_path() {
        let mut cfg = Cfg::default();
        cfg.audit_conf.remote_ca_path = Some("/tmp/does-not-exist-ca.pem".into());

        let err = ReadOnlyAuditRepository::build_client(&cfg).unwrap_err();
        assert_eq!(err.status, ErrorCode::InternalServerError);
        assert!(err
            .message
            .contains("Failed to read audit remote CA certificate"));
    }

    #[test]
    fn build_client_rejects_invalid_custom_ca_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("invalid-ca.pem");
        std::fs::write(
            &path,
            b"-----BEGIN CERTIFICATE-----\ninvalid-base64\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let mut cfg = Cfg::default();
        cfg.audit_conf.remote_ca_path = Some(path);

        let err = ReadOnlyAuditRepository::build_client(&cfg).unwrap_err();
        assert_eq!(err.status, ErrorCode::InternalServerError);
    }
}
