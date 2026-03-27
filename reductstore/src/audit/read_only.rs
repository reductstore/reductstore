// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{AuditEvent, AuditQuery, ManageAudit};
use crate::cfg::Cfg;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use log::warn;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::{forbidden, internal_server_error, unprocessable_entity};
use reqwest::header::HeaderValue;
use reqwest::{Certificate, Client, Response};
use std::sync::Arc;
use url::form_urlencoded;

static API_PATH: &str = "api/v1";

pub(crate) struct ReadOnlyAuditRepository {
    client: Client,
    primary_url: String,
}

impl ReadOnlyAuditRepository {
    pub async fn new(cfg: Cfg, _storage: Arc<StorageEngine>) -> Self {
        let primary_url = cfg
            .replication_conf
            .primary_url
            .clone()
            .map(|url| {
                if url.ends_with('/') {
                    url
                } else {
                    format!("{}/", url)
                }
            })
            .unwrap_or_default();

        let client = Self::build_client(&cfg);

        Self {
            client,
            primary_url,
        }
    }

    fn build_client(cfg: &Cfg) -> Client {
        let mut headers = reqwest::header::HeaderMap::new();
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
            match std::fs::read(ca_path) {
                Ok(cert_bytes) => match Certificate::from_pem(&cert_bytes) {
                    Ok(cert) => {
                        builder = builder.add_root_certificate(cert);
                    }
                    Err(err) => warn!(
                        "Ignoring invalid audit replica CA certificate '{}': {}",
                        ca_path.display(),
                        err
                    ),
                },
                Err(err) => warn!(
                    "Ignoring unreadable audit replica CA certificate '{}': {}",
                    ca_path.display(),
                    err
                ),
            }
        }

        builder.build().expect("audit replica client must build")
    }

    fn build_query_url(&self, token_name: &str, filter: &AuditQuery) -> String {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        if let Some(start) = filter.start {
            serializer.append_pair("start", &start.to_string());
        }
        if let Some(end) = filter.end {
            serializer.append_pair("end", &end.to_string());
        }
        if let Some(endpoint) = &filter.endpoint {
            serializer.append_pair("endpoint", endpoint);
        }

        let query = serializer.finish();
        if query.is_empty() {
            format!("{}{}/audit/{}", self.primary_url, API_PATH, token_name)
        } else {
            format!(
                "{}{}/audit/{}?{}",
                self.primary_url, API_PATH, token_name, query
            )
        }
    }
}

fn check_response(response: Result<Response, reqwest::Error>) -> Result<Response, ReductError> {
    let map_error = |error: reqwest::Error| -> ReductError {
        let status = if error.is_connect() {
            ErrorCode::ConnectionError
        } else if error.is_timeout() {
            ErrorCode::Timeout
        } else if error.is_request() {
            ErrorCode::BadRequest
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
        .and_then(|v| v.to_str().ok())
        .unwrap_or("Unknown");

    Err(ReductError::new(status, error_msg))
}

#[async_trait]
impl ManageAudit for ReadOnlyAuditRepository {
    async fn log_event(&mut self, _event: AuditEvent) -> Result<(), ReductError> {
        Err(forbidden!("Cannot write audit data in read-only mode"))
    }

    async fn query_token_events(
        &mut self,
        token_name: &str,
        filter: AuditQuery,
    ) -> Result<Vec<AuditEvent>, ReductError> {
        if self.primary_url.is_empty() {
            return Err(unprocessable_entity!(
                "Primary URL is not configured for replica audit reads"
            ));
        }

        let url = self.build_query_url(token_name, &filter);
        let response = check_response(self.client.get(url).send().await)?;
        let body = response
            .bytes()
            .await
            .map_err(|err| ReductError::new(ErrorCode::Unknown, &err.to_string()))?;

        serde_json::from_slice::<Vec<AuditEvent>>(&body)
            .map_err(|err| internal_server_error!("Failed to deserialize audit response: {}", err))
    }
}
