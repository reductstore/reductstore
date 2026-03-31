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
use std::time::Duration;
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

        let handler: FlushHandler = Arc::new(move |event| {
            let client = client.clone();
            let primary_url = primary_url.clone();
            let secondary_url = secondary_url.clone();
            let preferred_url = Arc::clone(&preferred_url);

            Box::pin(async move {
                Self::log_event_with_failover(
                    &client,
                    primary_url.as_deref(),
                    secondary_url.as_deref(),
                    preferred_url,
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

        Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(true)
            .http1_only()
            .build()
            .map_err(|err| {
                unprocessable_entity!("Failed to build audit replica HTTP client: {}", err)
            })
    }

    async fn log_event_with_failover(
        client: &Client,
        primary_url: Option<&str>,
        secondary_url: Option<&str>,
        preferred_url: Arc<AsyncRwLock<Option<String>>>,
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
            match Self::log_event_to_url(client, &base_url, event).await {
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
