// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use reduct_base::error::{ErrorCode, IntEnum, ReductError};
use reqwest::header::HeaderValue;
use reqwest::{Method, RequestBuilder, Response, Url};

/// Internal HTTP client to wrap reqwest.
pub(crate) struct HttpClient {
    base_url: Url,
    api_token: String,
    client: reqwest::Client,
}

impl HttpClient {
    pub(super) fn new(base_url: &str, api_token: &str, client: reqwest::Client) -> Result<Self> {
        Ok(Self {
            base_url: Url::parse(base_url)?,
            api_token: format!("Bearer {}", api_token),
            client,
        })
    }

    pub fn url(&self) -> &str {
        &self.base_url.as_str()[..self.base_url.as_str().len() - 7] // Remove /api/v1
    }

    pub fn api_token(&self) -> &str {
        &self.api_token[7..] // Remove Bearer
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
        let mut request = self.request(method, path);
        if let Some(body) = data {
            request = request.json(&body);
        }

        let response = self.send_request(request).await?;
        response.json::<Out>().await.map_err(|e| {
            ReductError::new(
                ErrorCode::Unknown,
                &format!("Failed to parse response: {}", e),
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
        let request = self.request(method, path).json(&data);
        self.send_request(request).await?;
        Ok(())
    }

    /// Prepare a request with the correct headers.
    pub fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client
            .request(method, format!("{}{}", self.base_url, path))
            .header("Authorization", &self.api_token)
    }

    /// Send a request and handle errors.
    pub async fn send_request(&self, request: RequestBuilder) -> Result<Response> {
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
                    Err(ReductError::new(status, &error_msg))
                }
            }
            Err(e) => Err(map_error(e)),
        };
        response
    }
}

/// Map reqwest errors to our own error type.
pub(crate) fn map_error(error: reqwest::Error) -> ReductError {
    let status = if error.is_connect() {
        ErrorCode::ConnectionError
    } else if error.is_timeout() {
        ErrorCode::Timeout
    } else {
        ErrorCode::Unknown
    };

    ReductError::new(status, &error.to_string())
}
