// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::{HeaderMap, Result};
use reduct_base::error::{ErrorCode, HttpError, IntEnum};
use reqwest::header::HeaderValue;
use reqwest::{Method, RequestBuilder, Response};

/// Internal HTTP client to wrap reqwest.
pub(crate) struct HttpClient {
    base_url: String,
    api_token: String,
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new(base_url: &str, api_token: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            api_token: format!("Bearer {}", api_token),
            client: reqwest::Client::new(),
        }
    }

    /// Send and receive JSON data from the server.
    ///
    /// # Arguments
    ///
    /// * `method` - HTTP method to use
    /// * `path` - Path to send request to
    /// * `data` - Optional data to send as JSON
    ///
    /// # Returns
    ///
    /// Deserialized JSON response
    ///
    /// # Errors
    ///
    /// * `HttpError` - If the request fails
    pub async fn send_and_receive_json<In, Out>(
        &self,
        method: Method,
        path: &str,
        data: Option<In>,
    ) -> Result<Out>
    where
        In: serde::Serialize,
        Out: for<'de> serde::Deserialize<'de>,
    {
        let mut request = self.prepare_request(method, &path);

        if let Some(body) = data {
            request = request.json(&body);
        }

        let response = Self::send_request(request).await?;
        response.json::<Out>().await.map_err(|e| {
            HttpError::new(
                ErrorCode::Unknown,
                &format!("Failed to parse response: {}", e.to_string()),
            )
        })
    }

    /// Send JSON data to the server.
    ///
    /// # Arguments
    ///
    /// * `method` - HTTP method to use
    /// * `path` - Path to send request to
    /// * `data` - Optional data to send as JSON
    ///
    /// # Errors
    ///
    /// * `HttpError` - If the request fails
    pub async fn send_json<In>(&self, method: Method, path: &str, data: In) -> Result<()>
    where
        In: serde::Serialize,
    {
        let mut request = self.prepare_request(method, &path);
        request = request.json(&data);
        Self::send_request(request).await?;
        Ok(())
    }

    /// HEAD request to the server.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to send request to
    ///
    /// # Returns
    ///
    /// * `HeaderMap` - Headers returned by the server
    pub async fn head(&self, path: &str) -> Result<HeaderMap> {
        let request = self.prepare_request(Method::HEAD, &path);
        let response = Self::send_request(request).await?;
        Ok(response
            .headers()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect::<HeaderMap>())
    }

    /// DELETE request to the server.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to send request to
    ///
    /// # Errors
    ///
    /// * `HttpError` - If the request fails
    pub async fn delete(&self, path: &str) -> Result<()> {
        let request = self.prepare_request(Method::DELETE, &path);
        Self::send_request(request).await?;
        Ok(())
    }

    fn prepare_request(&self, method: Method, path: &&str) -> RequestBuilder {
        let request = self
            .client
            .request(method, &format!("{}{}", self.base_url, path))
            .header("Authorization", &self.api_token);
        request
    }

    async fn send_request(request: RequestBuilder) -> Result<Response> {
        let response = match request.send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(response)
                } else {
                    let status = ErrorCode::from_int(response.status().as_u16() as i16)
                        .unwrap_or(ErrorCode::Unknown);
                    let error_msg = response
                        .headers()
                        .get("x-reduct-error")
                        .unwrap_or(&HeaderValue::from_str("Unknown").unwrap())
                        .to_str()
                        .unwrap()
                        .to_string();
                    Err(HttpError::new(status, &error_msg))
                }
            }
            Err(e) => {
                let status = if e.is_connect() {
                    ErrorCode::ConnectionError
                } else if e.is_timeout() {
                    ErrorCode::Timeout
                } else {
                    ErrorCode::Unknown
                };

                Err(HttpError::new(status, &e.to_string()))
            }
        };
        response
    }
}
