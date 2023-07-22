// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use reduct_base::error::{ErrorCode, HttpError, IntEnum};
use reqwest::header::HeaderValue;
use reqwest::{Method, Response, StatusCode};

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

    pub async fn request_json<In, Out>(
        &self,
        method: Method,
        path: &str,
        data: Option<In>,
    ) -> Result<Out>
    where
        In: serde::Serialize,
        Out: serde::de::DeserializeOwned,
    {
        let mut request = self
            .client
            .request(method, &format!("{}{}", self.base_url, path))
            .header("Authorization", &self.api_token);

        if let Some(body) = data {
            request = request.body(serde_json::to_string(&body).unwrap());
        }

        match request.send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(response
                        .json::<Out>()
                        .await
                        .map_err(|e| HttpError::new(ErrorCode::Unknown, &e.to_string()))?)
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
        }
    }
}
