// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod server_api;

/// ReductStore client.
pub struct ReductClient {
    http_client: reqwest::Client,
    url: String,
    api_token: String,
}

impl ReductClient {
    /// Create a new ReductClientBuilder.
    ///
    /// # Examples
    ///
    /// ```
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
}

pub struct ReductClientBuilder {
    url: String,
    api_token: String,
}

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
            http_client: reqwest::Client::new(),
            url: self.url,
            api_token: self.api_token,
        }
    }

    /// Set the URL of the ReductStore instance to connect to.
    pub fn set_url(mut self, url: &str) -> Self {
        self.url = url.to_string();
        self
    }

    /// Set the API token to use for authentication.
    pub fn set_api_token(mut self, api_token: &str) -> Self {
        self.api_token = api_token.to_string();
        self
    }
}
