use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::http_client::HttpClient;
use reduct_base::error::HttpError;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse, TokenList};

pub struct ReductClientBuilder {
    url: String,
    api_token: String,
}

pub type Result<T> = std::result::Result<T, HttpError>;

static API_BASE: &str = "/api/v1";

pub type HeaderMap = HashMap<String, String>;

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

    /// Get the server info.
    ///
    /// # Returns
    ///
    /// The server info
    pub async fn server_info(&self) -> Result<ServerInfo> {
        self.http_client
            .read()
            .unwrap()
            .request_json::<(), ServerInfo>(reqwest::Method::GET, "/info", None)
            .await
    }

    /// Get the bucket list.
    ///
    /// # Returns
    ///
    /// The bucket list.
    pub async fn bucket_list(&self) -> Result<BucketInfoList> {
        self.http_client
            .read()
            .unwrap()
            .request_json::<(), BucketInfoList>(reqwest::Method::GET, "/list", None)
            .await
    }

    /// Check if the server is alive.
    ///
    /// # Returns
    ///
    /// Ok if the server is alive, otherwise an error.
    pub async fn alive(&self) -> Result<()> {
        self.http_client.read().unwrap().head("/alive").await?;
        Ok(())
    }
    /// Get the token with permissions for the current user.
    ///
    /// # Returns
    ///
    /// The token or HttpError
    pub async fn me(&self) -> Result<Token> {
        self.http_client
            .read()
            .unwrap()
            .request_json::<(), Token>(reqwest::Method::GET, "/me", None)
            .await
    }

    /// Create a tokem
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the token
    /// * `permissions` - The permissions of the token
    ///
    /// # Returns
    ///
    /// The token value or HttpError
    pub async fn create_token(&self, name: &str, permissions: Permissions) -> Result<String> {
        let token = self
            .http_client
            .read()
            .unwrap()
            .request_json::<Permissions, TokenCreateResponse>(
                reqwest::Method::POST,
                &format!("/tokens/{}", name),
                Some(permissions),
            )
            .await?;
        Ok(token.value)
    }

    /// Delete a token
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the token
    ///
    /// # Returns
    ///
    /// Ok if the token was deleted, otherwise an error
    pub async fn delete_token(&self, name: &str) -> Result<()> {
        self.http_client
            .read()
            .unwrap()
            .delete(&format!("/tokens/{}", name))
            .await?;
        Ok(())
    }

    /// List all tokens
    ///
    /// # Returns
    ///
    /// The list of tokens or an error
    pub async fn list_tokens(&self) -> Result<Vec<Token>> {
        let list = self
            .http_client
            .read()
            .unwrap()
            .request_json::<(), TokenList>(reqwest::Method::GET, "/tokens", None)
            .await?;
        Ok(list.tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use tokio;

    #[rstest]
    #[tokio::test]
    async fn test_server_info(#[future] client: ReductClient) {
        let info = client.await.server_info().await.unwrap();
        assert!(info.version.starts_with("1."));
        assert!(info.bucket_count >= 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_list(#[future] client: ReductClient) {
        let info = client.await.bucket_list().await.unwrap();
        assert!(info.buckets.len() >= 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_alive(#[future] client: ReductClient) {
        client.await.alive().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_me(#[future] client: ReductClient) {
        let token = client.await.me().await.unwrap();
        assert_eq!(token.name, "init-token");
        assert!(token.permissions.unwrap().full_access);
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_token(#[future] client: ReductClient) {
        let token_value = client
            .await
            .create_token(
                "test-token",
                Permissions {
                    full_access: false,
                    read: vec!["test-bucket".to_string()],
                    write: vec!["test-bucket".to_string()],
                },
            )
            .await
            .unwrap();

        assert!(token_value.starts_with("test-token"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_tokens(#[future] client: ReductClient) {
        let tokens = client.await.list_tokens().await.unwrap();
        assert!(tokens.len() >= 1);
    }

    #[rstest]
    #[tokio::test]
    async fn delete_token(#[future] client: ReductClient) {
        let client = client.await;
        client
            .create_token("test-token", Permissions::default())
            .await
            .unwrap();
        client.delete_token("test-token").await.unwrap();
    }

    #[fixture]
    async fn client() -> ReductClient {
        let client = ReductClient::builder()
            .set_url("http://127.0.0.1:8383")
            .set_api_token(&std::env::var("RS_API_TOKEN").unwrap_or("".to_string()))
            .build();

        for token in client.list_tokens().await.unwrap() {
            if token.name.starts_with("test-token") {
                client.delete_token(&token.name).await.unwrap();
            }
        }

        client
    }
}
