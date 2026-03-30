// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{AuditEvent, ManageAudit, AUDIT_BUCKET_NAME};
use crate::cfg::Cfg;
use crate::core::sync::AsyncRwLock;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::unprocessable_entity;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Certificate, Client, Response};
use std::sync::Arc;
use url::form_urlencoded;

pub(crate) struct ReadOnlyAuditRepository {
    client: Client,
    primary_url: Option<String>,
    secondary_url: Option<String>,
    preferred_url: Arc<AsyncRwLock<Option<String>>>,
}

impl ReadOnlyAuditRepository {
    pub async fn new(cfg: Cfg, _storage: Arc<StorageEngine>) -> Self {
        let primary_url = normalize_url(cfg.primary_url.clone());
        let secondary_url = normalize_url(cfg.secondary_url.clone());
        let client = Self::build_client(&cfg).expect("audit replica client must build");

        Self {
            client,
            primary_url,
            secondary_url,
            preferred_url: Arc::new(AsyncRwLock::new(None)),
        }
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
            .connect_timeout(cfg.replication_conf.connection_timeout)
            .danger_accept_invalid_certs(!cfg.replication_conf.verify_ssl)
            .http1_only();

        if let Some(ca_path) = cfg.replication_conf.ca_path.as_ref() {
            let cert_bytes = std::fs::read(ca_path).map_err(|err| {
                unprocessable_entity!(
                    "Failed to read audit replica CA certificate '{}': {}",
                    ca_path.display(),
                    err
                )
            })?;
            let cert = Certificate::from_pem(&cert_bytes).map_err(|err| {
                unprocessable_entity!(
                    "Invalid audit replica CA certificate '{}': {}",
                    ca_path.display(),
                    err
                )
            })?;
            builder = builder.add_root_certificate(cert);
        }

        builder.build().map_err(|err| {
            unprocessable_entity!("Failed to build audit replica HTTP client: {}", err)
        })
    }

    async fn log_event_with_failover(&self, event: &AuditEvent) -> Result<(), ReductError> {
        let mut candidates = Vec::new();

        if let Some(url) = self.preferred_url.read().await?.clone() {
            candidates.push(url);
        }
        if let Some(url) = &self.primary_url {
            if !candidates.contains(url) {
                candidates.push(url.clone());
            }
        }
        if let Some(url) = &self.secondary_url {
            if !candidates.contains(url) {
                candidates.push(url.clone());
            }
        }

        if candidates.is_empty() {
            return Err(unprocessable_entity!(
                "Neither primary nor secondary URL is configured for replica audit writes"
            ));
        }

        let mut last_err = None;
        for base_url in candidates {
            match self.log_event_to_url(&base_url, event).await {
                Ok(()) => {
                    *self.preferred_url.write().await? = Some(base_url);
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
            ReductError::new(
                ErrorCode::Unknown,
                "Failed to write audit event to any replica URL",
            )
        }))
    }

    async fn log_event_to_url(
        &self,
        base_url: &str,
        event: &AuditEvent,
    ) -> Result<(), ReductError> {
        let url = build_write_url(base_url, event);
        let payload = serde_json::to_vec(event).map_err(|err| {
            ReductError::internal_server_error(&format!("Failed to serialize audit event: {}", err))
        })?;

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

        let response = self
            .client
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
        self.log_event_with_failover(&event).await
    }
}
