// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::Cfg;
use crate::core::internal_client::{
    ClientBuildErrorContext, ClientBuildErrorKind, InternalClientApi, InternalClientBuilder,
};
use crate::syslog::{LogSystemEvent, SystemEvent};
use async_trait::async_trait;
use bytes::Bytes;
use log::error;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::unprocessable_entity;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::Body;
use url::form_urlencoded;

pub(super) struct ForwardSystemLogger {
    bucket_name: &'static str,
    entry_prefix: Option<&'static str>,
    client_api: InternalClientApi,
}

impl ForwardSystemLogger {
    pub(super) fn new(
        bucket_name: &'static str,
        entry_prefix: Option<&'static str>,
        cfg: &Cfg,
    ) -> Result<Self, ReductError> {
        let (remote_verify_ssl, remote_ca_path, remote_timeout) = (
            cfg.system_events_conf.remote_verify_ssl,
            cfg.system_events_conf.remote_ca_path.as_ref(),
            cfg.system_events_conf.remote_timeout,
        );

        let client = InternalClientBuilder::new(ClientBuildErrorContext {
            ca_read: "Failed to read system bucket remote CA certificate",
            ca_parse: "Failed to parse system bucket remote CA certificate",
            client_build: "Failed to build system bucket replica HTTP client",
            kind: ClientBuildErrorKind::InternalServerError,
        })
        .api_token(&cfg.api_token)
        .verify_ssl(remote_verify_ssl)
        .ca_path(remote_ca_path)
        .connect_timeout(remote_timeout)
        .build()?;

        Ok(Self {
            bucket_name,
            entry_prefix,
            client_api: InternalClientApi::new(
                client,
                cfg.primary_url.clone(),
                cfg.secondary_url.clone(),
            ),
        })
    }

    #[cfg(test)]
    pub(super) fn new_with_client_api(
        bucket_name: &'static str,
        entry_prefix: Option<&'static str>,
        client_api: InternalClientApi,
    ) -> Self {
        Self {
            bucket_name,
            entry_prefix,
            client_api,
        }
    }

    async fn log_forward(&self, event: SystemEvent) -> Result<(), ReductError> {
        let primary_url = self.client_api.primary_url();
        let secondary_url = self.client_api.secondary_url();

        self.client_api
            .execute_with_failover_policy(
                "Neither primary nor secondary URL is configured for replica system bucket writes",
                |client, base_url| {
                    let event = event.clone();
                    async move {
                        let url = build_write_url(
                            base_url.as_str(),
                            self.bucket_name,
                            self.entry_prefix,
                            &event,
                        );
                        let payload = event.to_flat_json()?;
                        let headers = build_headers(&event, payload.len())?;

                        let response = client
                            .post(url)
                            .headers(headers)
                            .body(Body::from(Bytes::from(payload)))
                            .send()
                            .await;
                        crate::core::internal_client::check_response(response)?;
                        Ok(())
                    }
                },
                is_failover_candidate,
            )
            .await
            .map_err(|err| {
                error!(
                    "Replica forwarding to system bucket '{}' failed (primary='{:?}', secondary='{:?}'): {}",
                    self.bucket_name, primary_url, secondary_url, err
                );
                err
            })
    }
}

#[async_trait]
impl LogSystemEvent for ForwardSystemLogger {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
        self.log_forward(event).await
    }
}

fn is_failover_candidate(err: &ReductError) -> bool {
    if matches!(err.status, ErrorCode::ConnectionError | ErrorCode::Timeout) {
        return true;
    }

    let status_code = err.status as i16;
    (500..600).contains(&status_code)
}

fn build_headers(event: &SystemEvent, payload_len: usize) -> Result<HeaderMap, ReductError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&payload_len.to_string()).unwrap(),
    );
    headers.insert(
        "x-reduct-label-status",
        HeaderValue::from_str(&event.status.to_string()).map_err(|err| {
            unprocessable_entity!("Invalid event status label '{}': {}", event.status, err)
        })?,
    );
    Ok(headers)
}

fn build_write_url(
    base_url: &str,
    bucket_name: &str,
    entry_prefix: Option<&str>,
    event: &SystemEvent,
) -> String {
    let instance_name = if event.instance.is_empty() {
        "unknown"
    } else {
        &event.instance
    };
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("ts", &event.timestamp.to_string());
    let query = query.finish();
    let entry_path = match entry_prefix {
        Some(prefix) => format!("{}/{}/{}", prefix, instance_name, event.entry_name),
        None => format!("{}/{}", instance_name, event.entry_name),
    };

    format!(
        "{}api/v1/b/{}/{}?{}",
        base_url, bucket_name, entry_path, query
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Request, State};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use axum::Router;
    use reduct_base::error::ErrorCode;
    use reqwest::header::HeaderName;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    #[derive(Clone, Debug)]
    struct CapturedRequest {
        path_and_query: String,
        content_type: Option<String>,
        content_length: Option<String>,
        label_status: Option<String>,
    }

    #[derive(Clone)]
    struct MockState {
        status: StatusCode,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
    }

    fn make_event() -> SystemEvent {
        SystemEvent {
            event_type: "api_call".to_string(),
            timestamp: 42,
            instance: "replica-a".to_string(),
            entry_name: "entry-1".to_string(),
            status: 201,
            message: "created".to_string(),
            payload: serde_json::json!({
                "method": "POST",
                "path": "/api/v1/b/data/entry-1",
                "call_count": 1,
            }),
        }
    }

    fn test_client_api(
        primary_url: Option<String>,
        secondary_url: Option<String>,
    ) -> InternalClientApi {
        InternalClientApi::new(reqwest::Client::new(), primary_url, secondary_url)
    }

    async fn mock_write_handler(State(state): State<MockState>, request: Request) -> Response {
        let path_and_query = request
            .uri()
            .path_and_query()
            .map(|path| path.as_str().to_string())
            .unwrap_or_default();
        let content_type = header_value(&request, CONTENT_TYPE);
        let content_length = header_value(&request, CONTENT_LENGTH);
        let label_status = header_value(&request, HeaderName::from_static("x-reduct-label-status"));

        state.requests.lock().unwrap().push(CapturedRequest {
            path_and_query,
            content_type,
            content_length,
            label_status,
        });

        if state.status.is_success() {
            return state.status.into_response();
        }

        let mut response = state.status.into_response();
        response.headers_mut().insert(
            HeaderName::from_static("x-reduct-error"),
            HeaderValue::from_static("mock error"),
        );
        response
    }

    fn header_value(request: &Request, header: HeaderName) -> Option<String> {
        request
            .headers()
            .get(header)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string)
    }

    async fn start_mock_server(
        status: StatusCode,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
    ) -> String {
        let app = Router::new()
            .route("/api/v1/b/{bucket}/{*entry}", post(mock_write_handler))
            .with_state(MockState { status, requests });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        format!("http://{}/", addr)
    }

    async fn start_logger(
        primary_status: StatusCode,
        primary_requests: Arc<Mutex<Vec<CapturedRequest>>>,
        secondary_url: Option<String>,
    ) -> ForwardSystemLogger {
        let primary_url = start_mock_server(primary_status, primary_requests).await;
        ForwardSystemLogger::new_with_client_api(
            "$system",
            Some("audit"),
            test_client_api(Some(primary_url), secondary_url),
        )
    }

    #[tokio::test]
    async fn test_forwards_event_to_primary() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut logger = start_logger(StatusCode::OK, Arc::clone(&requests), None).await;

        logger.log_event(make_event()).await.unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].path_and_query,
            "/api/v1/b/$system/audit/replica-a/entry-1?ts=42"
        );
    }

    #[tokio::test]
    async fn test_failover_to_secondary_on_server_error() {
        let primary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_url =
            start_mock_server(StatusCode::OK, Arc::clone(&secondary_requests)).await;
        let mut logger = start_logger(
            StatusCode::INTERNAL_SERVER_ERROR,
            Arc::clone(&primary_requests),
            Some(secondary_url),
        )
        .await;

        logger.log_event(make_event()).await.unwrap();

        assert_eq!(primary_requests.lock().unwrap().len(), 1);
        assert_eq!(secondary_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_both_urls_fail_returns_error() {
        let primary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_url =
            start_mock_server(StatusCode::BAD_GATEWAY, Arc::clone(&secondary_requests)).await;
        let mut logger = start_logger(
            StatusCode::INTERNAL_SERVER_ERROR,
            Arc::clone(&primary_requests),
            Some(secondary_url),
        )
        .await;

        let err = logger.log_event(make_event()).await.err().unwrap();

        assert_eq!(err.status, ErrorCode::BadGateway);
        assert_eq!(err.message, "mock error");
        assert_eq!(primary_requests.lock().unwrap().len(), 1);
        assert_eq!(secondary_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_no_failover_on_client_error() {
        let primary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_url =
            start_mock_server(StatusCode::OK, Arc::clone(&secondary_requests)).await;
        let mut logger = start_logger(
            StatusCode::UNPROCESSABLE_ENTITY,
            Arc::clone(&primary_requests),
            Some(secondary_url),
        )
        .await;

        let err = logger.log_event(make_event()).await.err().unwrap();

        assert_eq!(err.status, ErrorCode::UnprocessableEntity);
        assert_eq!(primary_requests.lock().unwrap().len(), 1);
        assert_eq!(secondary_requests.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_connection_error_triggers_failover() {
        let secondary_requests = Arc::new(Mutex::new(Vec::new()));
        let secondary_url =
            start_mock_server(StatusCode::OK, Arc::clone(&secondary_requests)).await;
        let mut logger = ForwardSystemLogger::new_with_client_api(
            "$system",
            Some("audit"),
            test_client_api(Some("http://127.0.0.1:1/".to_string()), Some(secondary_url)),
        );

        logger.log_event(make_event()).await.unwrap();

        assert_eq!(secondary_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_no_urls_configured_returns_error() {
        let mut logger = ForwardSystemLogger::new_with_client_api(
            "$system",
            Some("audit"),
            test_client_api(None, None),
        );

        let err = logger.log_event(make_event()).await.err().unwrap();

        assert_eq!(err.status, ErrorCode::UnprocessableEntity);
        assert_eq!(
            err.message,
            "Neither primary nor secondary URL is configured for replica system bucket writes"
        );
    }

    #[tokio::test]
    async fn test_request_headers_correct() {
        let event = make_event();
        let payload_len = event.to_flat_json().unwrap().len().to_string();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut logger = start_logger(StatusCode::OK, Arc::clone(&requests), None).await;

        logger.log_event(event).await.unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[0].content_type.as_deref(),
            Some("application/json")
        );
        assert_eq!(
            requests[0].content_length.as_deref(),
            Some(payload_len.as_str())
        );
        assert_eq!(requests[0].label_status.as_deref(), Some("201"));
    }

    #[tokio::test]
    async fn test_url_construction_with_entry_name() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut logger = start_logger(StatusCode::OK, Arc::clone(&requests), None).await;
        let mut event = make_event();
        event.timestamp = 1_700_000_000_000_000;
        event.instance = "node-1".to_string();
        event.entry_name = "startup".to_string();

        logger.log_event(event).await.unwrap();

        assert_eq!(
            requests.lock().unwrap()[0].path_and_query,
            "/api/v1/b/$system/audit/node-1/startup?ts=1700000000000000"
        );
    }

    #[tokio::test]
    async fn test_empty_instance_uses_unknown() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut logger = start_logger(StatusCode::OK, Arc::clone(&requests), None).await;
        let mut event = make_event();
        event.instance.clear();

        logger.log_event(event).await.unwrap();

        assert_eq!(
            requests.lock().unwrap()[0].path_and_query,
            "/api/v1/b/$system/audit/unknown/entry-1?ts=42"
        );
    }

    #[test]
    fn test_build_headers_sets_content_type_and_length() {
        let event = make_event();

        let headers = build_headers(&event, 123).unwrap();

        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), "123");
        assert_eq!(headers.get("x-reduct-label-status").unwrap(), "201");
    }

    #[test]
    fn test_build_write_url_formats_correctly() {
        let event = make_event();

        let url = build_write_url("http://127.0.0.1:8383/", "$system", Some("audit"), &event);

        assert_eq!(
            url,
            "http://127.0.0.1:8383/api/v1/b/$system/audit/replica-a/entry-1?ts=42"
        );
    }

    #[test]
    fn test_build_write_url_empty_instance_uses_unknown() {
        let mut event = make_event();
        event.instance.clear();

        let url = build_write_url("http://127.0.0.1:8383/", "$system", Some("audit"), &event);

        assert_eq!(
            url,
            "http://127.0.0.1:8383/api/v1/b/$system/audit/unknown/entry-1?ts=42"
        );
    }

    #[test]
    fn test_is_failover_candidate_connection_error() {
        let err = ReductError::new(ErrorCode::ConnectionError, "connection refused");

        assert!(is_failover_candidate(&err));
    }

    #[test]
    fn test_is_failover_candidate_timeout() {
        let err = ReductError::new(ErrorCode::Timeout, "timeout");

        assert!(is_failover_candidate(&err));
    }

    #[test]
    fn test_is_failover_candidate_5xx() {
        let err = ReductError::new(ErrorCode::InternalServerError, "server error");

        assert!(is_failover_candidate(&err));
    }

    #[test]
    fn test_is_failover_candidate_4xx_not_failover() {
        let err = ReductError::new(ErrorCode::UnprocessableEntity, "client error");

        assert!(!is_failover_candidate(&err));
    }
}
