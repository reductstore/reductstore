// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reqwest::Method;

use std::sync::Arc;

use crate::http_client::HttpClient;
use crate::Bucket;
use reduct_base::error::{ErrorCode, HttpError};
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse, TokenList};

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
            http_client: Arc::new(HttpClient::new(&self.url, &self.api_token)),
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
    http_client: Arc<HttpClient>,
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
            .send_and_receive_json::<(), ServerInfo>(reqwest::Method::GET, "/info", None)
            .await
    }

    /// Get the bucket list.
    ///
    /// # Returns
    ///
    /// The bucket list.
    pub async fn bucket_list(&self) -> Result<BucketInfoList> {
        self.http_client
            .send_and_receive_json::<(), BucketInfoList>(Method::GET, "/list", None)
            .await
    }

    /// Create a bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    /// * `settings` - The settings of the bucket
    /// * `exists_ok` - If true, return Ok if the bucket already exists
    ///
    /// # Returns
    ///
    /// the created bucket or an error
    pub async fn create_bucket(&self, name: &str, settings: BucketSettings) -> Result<Bucket> {
        self.http_client
            .send_json(Method::POST, &format!("/b/{}", name), settings)
            .await?;
        Ok(Bucket {
            name: name.to_string(),
            http_client: self.http_client.clone(),
        })
    }

    /// Get a bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    ///
    /// the bucket or an error
    pub async fn get_bucket(&self, name: &str) -> Result<Bucket> {
        let request = self
            .http_client
            .request(Method::HEAD, &format!("/b/{}", name));
        self.http_client.send_request(request).await?;
        Ok(Bucket {
            name: name.to_string(),
            http_client: self.http_client.clone(),
        })
    }

    /// Get or create a bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    /// * `settings` - The settings of the bucket
    ///
    /// # Returns
    ///
    /// the bucket or an error
    pub async fn get_or_create_bucket(
        &self,
        name: &str,
        settings: BucketSettings,
    ) -> Result<Bucket> {
        match self.get_bucket(name).await {
            Ok(bucket) => Ok(bucket),
            Err(err) => {
                if err.status == ErrorCode::NotFound {
                    self.create_bucket(name, settings).await
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Check if the server is alive.
    ///
    /// # Returns
    ///
    /// Ok if the server is alive, otherwise an error.
    pub async fn alive(&self) -> Result<()> {
        let request = self.http_client.request(Method::HEAD, "/alive");
        self.http_client.send_request(request).await?;
        Ok(())
    }
    /// Get the token with permissions for the current user.
    ///
    /// # Returns
    ///
    /// The token or HttpError
    pub async fn me(&self) -> Result<Token> {
        self.http_client
            .send_and_receive_json::<(), Token>(reqwest::Method::GET, "/me", None)
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
            .send_and_receive_json::<Permissions, TokenCreateResponse>(
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
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/tokens/{}", name));
        self.http_client.send_request(request).await?;
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
            .send_and_receive_json::<(), TokenList>(reqwest::Method::GET, "/tokens", None)
            .await?;
        Ok(list.tokens)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::record::Labels;
    use bytes::Bytes;
    use reduct_base::msg::bucket_api::QuotaType;
    use rstest::{fixture, rstest};
    use tokio;

    mod serve_api {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_server_info(#[future] client: ReductClient) {
            let info = client.await.server_info().await.unwrap();
            assert!(info.version.starts_with("1."));
            assert!(info.bucket_count >= 2);
        }

        #[rstest]
        #[tokio::test]
        async fn test_bucket_list(#[future] client: ReductClient) {
            let info = client.await.bucket_list().await.unwrap();
            assert!(info.buckets.len() >= 2);
        }

        #[rstest]
        #[tokio::test]
        async fn test_alive(#[future] client: ReductClient) {
            client.await.alive().await.unwrap();
        }
    }

    mod bucket_api {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_create_bucket(#[future] client: ReductClient) {
            let client = client.await;
            let bucket = client
                .create_bucket("test-bucket", BucketSettings::default())
                .await
                .unwrap();
            assert_eq!(bucket.name(), "test-bucket");
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_or_bucket(#[future] client: ReductClient) {
            let client = client.await;
            let bucket = client
                .get_or_create_bucket("test-bucket", BucketSettings::default())
                .await
                .unwrap();
            assert_eq!(bucket.name(), "test-bucket");
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_bucket(#[future] client: ReductClient) {
            let client = client.await;
            let bucket = client.get_bucket("test-bucket-1").await.unwrap();
            assert_eq!(bucket.name(), "test-bucket-1");
        }
    }

    mod token_api {
        use super::*;

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
    }

    #[fixture]
    pub(crate) fn bucket_settings() -> BucketSettings {
        BucketSettings {
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(100),
            max_block_size: Some(512),
            max_block_records: Some(100),
        }
    }

    #[fixture]
    pub(crate) async fn client(bucket_settings: BucketSettings) -> ReductClient {
        let client = ReductClient::builder()
            .set_url("http://127.0.0.1:8383")
            .set_api_token(&std::env::var("RS_API_TOKEN").unwrap_or("".to_string()))
            .build();

        for token in client.list_tokens().await.unwrap() {
            if token.name.starts_with("test-token") {
                client.delete_token(&token.name).await.unwrap();
            }
        }

        for bucket in client.bucket_list().await.unwrap().buckets {
            if bucket.name.starts_with("test-bucket") {
                let bucket = client.get_bucket(&bucket.name).await.unwrap();
                bucket.remove().await.unwrap();
            }
        }

        let bucket = client
            .create_bucket("test-bucket-1", bucket_settings.clone())
            .await
            .unwrap();
        bucket
            .write_record("entry-1")
            .timestamp_us(1000)
            .content_type("text/plain")
            .labels(Labels::from([
                ("entry".into(), "1".into()),
                ("bucket".into(), "1".into()),
            ]))
            .data(Bytes::from("Hey entry-1!"))
            .send()
            .await
            .unwrap();

        bucket
            .write_record("entry-2")
            .timestamp_us(2000)
            .content_type("text/plain")
            .labels(Labels::from([
                ("entry".into(), "2".into()),
                ("bucket".into(), "1".into()),
            ]))
            .data(Bytes::from("Hey entry-2!"))
            .send()
            .await
            .unwrap();

        let bucket = client
            .create_bucket("test-bucket-2", bucket_settings)
            .await
            .unwrap();

        bucket
            .write_record("entry-1")
            .timestamp_us(1000)
            .labels(Labels::from([
                ("entry".into(), "1".into()),
                ("bucket".into(), "2".into()),
            ]))
            .data(Bytes::from("Hey entry-1!"))
            .send()
            .await
            .unwrap();

        bucket
            .write_record("entry-2")
            .timestamp_us(2000)
            .labels(Labels::from([
                ("entry".into(), "2".into()),
                ("bucket".into(), "2".into()),
            ]))
            .data(Bytes::from("Hey entry-2!"))
            .send()
            .await
            .unwrap();

        client
    }
}
