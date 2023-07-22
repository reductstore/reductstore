use serde::de::Unexpected::Unit;
use std::sync::{Arc, RwLock};
use std::time::SystemTimeError;
// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::http_client::HttpClient;
use reduct_base::error::HttpError;
use reduct_base::msg::server_api::ServerInfo;

pub struct ReductClientBuilder {
    url: String,
    api_token: String,
}

pub type Result<T> = std::result::Result<T, HttpError>;

static API_BASE: &str = "/api/v1";

impl ReductClientBuilder {
    fn new() -> Self {
        Self {
            url: String::new(),
            api_token: String::new(),
        }
    }

    /// Build the ReductClient.
    ///
    /// # Panics
    ///
    /// Panics if the URL is not set.
    pub fn build(self) -> ReductClient {
        assert!(!self.url.is_empty(), "URL must be set");
        ReductClient {
            http_client: Arc::new(RwLock::new(HttpClient::new(&self.url, &self.api_token))),
        }
    }

    /// Set the URL of the ReductStore instance to connect to.
    pub fn set_url(mut self, url: &str) -> Self {
        self.url = format!("{}{}", url.to_string(), API_BASE);
        self
    }

    /// Set the API token to use for authentication.
    pub fn set_api_token(mut self, api_token: &str) -> Self {
        self.api_token = api_token.to_string();
        self
    }
}

/// ReductStore client.
pub struct ReductClient {
    http_client: Arc<RwLock<HttpClient>>,
}

impl ReductClient {
    /// Create a new ReductClientBuilder.
    ///
    /// # Examples
    ///
    /// ```unwrap
    /// use reduct_rs::ReductClient;
    ///
    /// let client = ReductClient::builder()
    ///    .set_url("https://reductstore.com")
    ///    .set_api_token("my-api-token")
    ///    .build();
    /// ```
    pub fn builder() -> ReductClientBuilder {
        ReductClientBuilder::new()
    }

    pub async fn server_info(self) -> Result<ServerInfo> {
        self.http_client
            .read()
            .unwrap()
            .request_json::<(), ServerInfo>(reqwest::Method::GET, "/info", None)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_server_info() {
        let client = ReductClient::builder()
            .set_url("http://127.0.0.1:8383")
            .set_api_token("my-api-token")
            .build();

        let info = client.server_info().await.unwrap();
        assert!(info.version.starts_with("1."));
        assert!(info.bucket_count >= 0);
    }
}
