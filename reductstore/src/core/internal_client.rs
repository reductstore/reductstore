// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::sync::AsyncRwLock;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::{internal_server_error, unprocessable_entity};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Response};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy)]
pub(crate) enum ClientBuildErrorKind {
    InternalServerError,
    UnprocessableEntity,
}

pub(crate) struct ClientBuildErrorContext<'a> {
    pub ca_read: &'a str,
    pub ca_parse: &'a str,
    pub client_build: &'a str,
    pub kind: ClientBuildErrorKind,
}

pub(crate) struct InternalClientBuilder<'a> {
    api_token: &'a str,
    verify_ssl: bool,
    ca_path: Option<&'a PathBuf>,
    connect_timeout: Duration,
    error_context: ClientBuildErrorContext<'a>,
}

impl<'a> InternalClientBuilder<'a> {
    pub(crate) fn new(error_context: ClientBuildErrorContext<'a>) -> Self {
        Self {
            api_token: "",
            verify_ssl: true,
            ca_path: None,
            connect_timeout: Duration::from_secs(10),
            error_context,
        }
    }

    pub(crate) fn api_token(mut self, api_token: &'a str) -> Self {
        self.api_token = api_token;
        self
    }

    pub(crate) fn verify_ssl(mut self, verify_ssl: bool) -> Self {
        self.verify_ssl = verify_ssl;
        self
    }

    pub(crate) fn ca_path(mut self, ca_path: Option<&'a PathBuf>) -> Self {
        self.ca_path = ca_path;
        self
    }

    pub(crate) fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    pub(crate) fn build(self) -> Result<Client, ReductError> {
        let mut headers = HeaderMap::new();
        if !self.api_token.is_empty() {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", self.api_token)).unwrap();
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }

        let mut builder = Client::builder()
            .default_headers(headers)
            .connect_timeout(self.connect_timeout)
            .danger_accept_invalid_certs(!self.verify_ssl)
            .http1_only();

        if let Some(path) = self.ca_path {
            let cert_data = std::fs::read(path).map_err(|err| {
                build_error(
                    self.error_context.kind,
                    format!("{} {}: {}", self.error_context.ca_read, path.display(), err),
                )
            })?;
            let cert = reqwest::Certificate::from_pem(&cert_data).map_err(|err| {
                build_error(
                    self.error_context.kind,
                    format!(
                        "{} {}: {}",
                        self.error_context.ca_parse,
                        path.display(),
                        err
                    ),
                )
            })?;
            builder = builder.add_root_certificate(cert);
        }

        builder.build().map_err(|err| {
            build_error(
                self.error_context.kind,
                format!("{}: {}", self.error_context.client_build, err),
            )
        })
    }
}

#[derive(Clone)]
pub(crate) struct InternalClientApi {
    client: Client,
    primary_url: Option<String>,
    secondary_url: Option<String>,
    preferred_url: Arc<AsyncRwLock<Option<String>>>,
}

impl InternalClientApi {
    pub(crate) fn new(
        client: Client,
        primary_url: Option<String>,
        secondary_url: Option<String>,
    ) -> Self {
        Self {
            client,
            primary_url: normalize_url(primary_url),
            secondary_url: normalize_url(secondary_url),
            preferred_url: Arc::new(AsyncRwLock::new(None)),
        }
    }

    pub(crate) fn with_single_url(client: Client, url: String) -> Self {
        Self::new(client, Some(url), None)
    }

    pub(crate) fn new_with_preferred(
        client: Client,
        primary_url: Option<String>,
        secondary_url: Option<String>,
        preferred_url: Arc<AsyncRwLock<Option<String>>>,
    ) -> Self {
        Self {
            client,
            primary_url: normalize_url(primary_url),
            secondary_url: normalize_url(secondary_url),
            preferred_url,
        }
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    pub(crate) fn primary_url(&self) -> Option<&str> {
        self.primary_url.as_deref()
    }

    pub(crate) fn secondary_url(&self) -> Option<&str> {
        self.secondary_url.as_deref()
    }

    pub(crate) fn preferred_url_handle(&self) -> Arc<AsyncRwLock<Option<String>>> {
        Arc::clone(&self.preferred_url)
    }

    pub(crate) async fn execute_with_failover<T, F, Fut>(
        &self,
        missing_url_error: &str,
        request: F,
    ) -> Result<T, ReductError>
    where
        F: FnMut(Client, String) -> Fut,
        Fut: Future<Output = Result<T, ReductError>>,
    {
        self.execute_with_failover_policy(missing_url_error, request, is_failover_candidate)
            .await
    }

    pub(crate) async fn execute_with_failover_policy<T, F, Fut, P>(
        &self,
        missing_url_error: &str,
        mut request: F,
        should_failover: P,
    ) -> Result<T, ReductError>
    where
        F: FnMut(Client, String) -> Fut,
        Fut: Future<Output = Result<T, ReductError>>,
        P: Fn(&ReductError) -> bool,
    {
        let candidates = self.candidates().await?;
        if candidates.is_empty() {
            return Err(unprocessable_entity!("{}", missing_url_error));
        }

        let mut last_err = None;
        for base_url in candidates {
            match request(self.client.clone(), base_url.clone()).await {
                Ok(result) => {
                    *self.preferred_url.write().await? = Some(base_url);
                    return Ok(result);
                }
                Err(err) => {
                    if !should_failover(&err) {
                        return Err(err);
                    }
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| unprocessable_entity!("{}", missing_url_error)))
    }

    async fn candidates(&self) -> Result<Vec<String>, ReductError> {
        let mut candidates = Vec::new();

        if let Some(url) = self.preferred_url.read().await?.clone() {
            candidates.push(url);
        }
        if let Some(url) = &self.primary_url {
            if !candidates.iter().any(|candidate| candidate == url) {
                candidates.push(url.clone());
            }
        }
        if let Some(url) = &self.secondary_url {
            if !candidates.iter().any(|candidate| candidate == url) {
                candidates.push(url.clone());
            }
        }

        Ok(candidates)
    }
}

pub(crate) fn normalize_url(url: Option<String>) -> Option<String> {
    url.map(|url| {
        if url.ends_with('/') {
            url
        } else {
            format!("{}/", url)
        }
    })
}

pub(crate) fn is_failover_candidate(err: &ReductError) -> bool {
    matches!(err.status, ErrorCode::ConnectionError | ErrorCode::Timeout)
}

pub(crate) fn check_response(
    response: Result<Response, reqwest::Error>,
) -> Result<Response, ReductError> {
    let response = response.map_err(map_request_error)?;
    if response.status().is_success() {
        return Ok(response);
    }

    let status =
        ErrorCode::try_from(response.status().as_u16() as i16).unwrap_or(ErrorCode::Unknown);

    let error_msg = response
        .headers()
        .get("x-reduct-error")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("Unknown")
        .to_string();

    Err(ReductError::new(status, &error_msg))
}

pub(crate) fn map_request_error(error: reqwest::Error) -> ReductError {
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
}

fn build_error(kind: ClientBuildErrorKind, message: String) -> ReductError {
    match kind {
        ClientBuildErrorKind::InternalServerError => internal_server_error!("{}", message),
        ClientBuildErrorKind::UnprocessableEntity => unprocessable_entity!("{}", message),
    }
}
