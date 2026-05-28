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
    client_api: InternalClientApi,
}

impl ForwardSystemLogger {
    pub(super) fn new(bucket_name: &'static str, cfg: &Cfg) -> Result<Self, ReductError> {
        let client = InternalClientBuilder::new(ClientBuildErrorContext {
            ca_read: "Failed to read system bucket remote CA certificate",
            ca_parse: "Failed to parse system bucket remote CA certificate",
            client_build: "Failed to build system bucket replica HTTP client",
            kind: ClientBuildErrorKind::InternalServerError,
        })
        .api_token(&cfg.api_token)
        .verify_ssl(cfg.audit_conf.remote_verify_ssl)
        .ca_path(cfg.audit_conf.remote_ca_path.as_ref())
        .connect_timeout(cfg.audit_conf.remote_timeout)
        .build()?;

        Ok(Self {
            bucket_name,
            client_api: InternalClientApi::new(
                client,
                cfg.primary_url.clone(),
                cfg.secondary_url.clone(),
            ),
        })
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
                        let url = build_write_url(base_url.as_str(), self.bucket_name, &event);
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

fn build_write_url(base_url: &str, bucket_name: &str, event: &SystemEvent) -> String {
    let instance_name = if event.instance.is_empty() {
        "unknown"
    } else {
        &event.instance
    };
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("ts", &event.timestamp.to_string());
    let query = query.finish();
    format!(
        "{}api/v1/b/{}/{}/{}?{}",
        base_url, bucket_name, instance_name, event.entry_name, query
    )
}
