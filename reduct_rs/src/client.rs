// Copyright 2023-2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reqwest::{Method, Url};

use std::sync::Arc;
use std::time::Duration;

use crate::bucket::BucketBuilder;
use crate::http_client::HttpClient;
use crate::Bucket;
use reduct_base::error::{ErrorCode, ReductError};

use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationInfo, ReplicationList, ReplicationSettings,
};

use crate::replication::ReplicationBuilder;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse, TokenList};

pub struct ReductClientBuilder {
    url: String,
    api_token: String,
    timeout: Duration,
    http1_only: bool,
}

pub type Result<T> = std::result::Result<T, ReductError>;

static API_BASE: &str = "/api/v1";

impl ReductClientBuilder {
    fn new() -> Self {
        Self {
            url: String::new(),
            api_token: String::new(),
            timeout: Duration::from_secs(30),
            http1_only: false,
        }
    }

    /// Build the ReductClient.
    ///
    /// # Panics
    ///
    /// Panics if the URL is not set or invalid.
    pub fn build(self) -> ReductClient {
        self.try_build().unwrap()
    }

    /// Try to build the ReductClient.
    pub fn try_build(self) -> Result<ReductClient> {
        if self.url.is_empty() {
            return Err(ReductError::new(
                ErrorCode::UrlParseError,
                "URL must be set",
            ));
        }
        ReductError::new(ErrorCode::UrlParseError, "URL must be set");
        let builder = reqwest::ClientBuilder::new().timeout(self.timeout);
        let builder = if self.http1_only {
            builder.http1_only()
        } else {
            builder
        };
        Ok(ReductClient {
            http_client: Arc::new(HttpClient::new(
                &self.url,
                &self.api_token,
                builder.build().map_err(|e| {
                    ReductError::new(
                        ErrorCode::Unknown,
                        &format!("Failed to create HTTP client: {}", e),
                    )
                })?,
            )?),
        })
    }

    /// Set the URL of the ReductStore instance to connect to.
    pub fn url(mut self, url: &str) -> Self {
        let url = Url::parse(url).expect("Invalid URL");
        self.url = format!("{}{}", url.origin().ascii_serialization(), API_BASE);
        self
    }

    /// Set the API token to use for authentication.
    pub fn api_token(mut self, api_token: &str) -> Self {
        self.api_token = api_token.to_string();
        self
    }

    /// Set the timeout for HTTP requests.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the HTTP version to HTTP/1.1 only.
    pub fn http1_only(mut self) -> Self {
        self.http1_only = true;
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

    /// Get the URL of the ReductStore instance.
    pub fn url(&self) -> &str {
        self.http_client.url()
    }

    /// Get the API token.
    pub fn api_token(&self) -> &str {
        self.http_client.api_token()
    }

    /// Get the server info.
    ///
    /// # Returns
    ///
    /// The server info
    pub async fn server_info(&self) -> Result<ServerInfo> {
        self.http_client
            .send_and_receive_json::<(), ServerInfo>(Method::GET, "/info", None)
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
    ///
    /// # Returns
    ///
    /// a bucket builder to set the bucket settings
    pub fn create_bucket(&self, name: &str) -> BucketBuilder {
        BucketBuilder::new(name.to_string(), Arc::clone(&self.http_client))
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
            .send_and_receive_json::<(), Token>(Method::GET, "/me", None)
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
                Method::POST,
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
            .send_and_receive_json::<(), TokenList>(Method::GET, "/tokens", None)
            .await?;
        Ok(list.tokens)
    }

    /// Get list of replications
    ///
    /// # Returns
    ///
    /// The list of replications or an error
    pub async fn list_replications(&self) -> Result<Vec<ReplicationInfo>> {
        let list = self
            .http_client
            .send_and_receive_json::<(), ReplicationList>(Method::GET, "/replications", None)
            .await?;
        Ok(list.replications)
    }

    /// Get full replication info
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the replication
    ///
    /// # Returns
    ///
    /// The replication info or an error
    pub async fn get_replication(&self, name: &str) -> Result<FullReplicationInfo> {
        let info = self
            .http_client
            .send_and_receive_json::<(), FullReplicationInfo>(
                Method::GET,
                &format!("/replications/{}", name),
                None,
            )
            .await?;
        Ok(info)
    }

    /// Create a replication
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the replication
    ///
    /// # Returns
    ///
    /// a replication builder to set the replication settings
    ///
    /// # Example
    ///
    /// ```no_run
    /// use reduct_rs::ReductClient;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = ReductClient::builder()
    ///         .url("http://127.0.0.1:8383")
    ///         .api_token("my-api-token")
    ///         .build();
    ///
    ///     client.create_replication("test-replication")
    ///         .src_bucket("test-bucket-1")
    ///         .dst_bucket("test-bucket-2")
    ///         .dst_host("https://play.reduct.store")
    ///         .dst_token("reductstore")
    ///         .send()
    ///         .await
    ///         .unwrap();
    /// }
    ///
    pub fn create_replication(&self, name: &str) -> ReplicationBuilder {
        ReplicationBuilder::new(name.to_string(), Arc::clone(&self.http_client))
    }

    /// Update a replication
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the replication
    /// * `settings` - The replication settings
    ///
    /// # Returns
    ///
    /// Ok if the replication was updated, otherwise an error
    pub async fn update_replication(
        &self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<()> {
        self.http_client
            .send_json(Method::PUT, &format!("/replications/{}", name), settings)
            .await
    }

    /// Delete a replication
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the replication
    ///
    /// # Returns
    ///
    /// Ok if the replication was deleted, otherwise an error
    pub async fn delete_replication(&self, name: &str) -> Result<()> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/replications/{}", name));
        self.http_client.send_request(request).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::record::Labels;
    use bytes::Bytes;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::{fixture, rstest};

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
            let bucket = client.create_bucket("test-bucket").send().await.unwrap();
            assert_eq!(bucket.name(), "test-bucket");
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_or_create_bucket(#[future] client: ReductClient) {
            let client = client.await;
            let bucket = client
                .create_bucket("test-bucket")
                .exist_ok(true)
                .send()
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
            assert!(!tokens.is_empty());
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

    mod replication_api {
        use super::*;
        use reduct_base::msg::diagnostics::Diagnostics;
        use reduct_base::msg::replication_api::ReplicationSettings;

        #[rstest]
        #[tokio::test]
        async fn test_list_replications(#[future] client: ReductClient) {
            let replications = client.await.list_replications().await.unwrap();
            assert!(replications.is_empty());
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_replication(
            #[future] client: ReductClient,
            settings: ReplicationSettings,
        ) {
            let client = client.await;
            client
                .create_replication("test-replication")
                .src_bucket(settings.src_bucket.as_str())
                .dst_bucket(settings.dst_bucket.as_str())
                .dst_host(settings.dst_host.as_str())
                .dst_token(settings.dst_token.as_str())
                .entries(settings.entries.clone())
                .include(settings.include.clone())
                .exclude(settings.exclude.clone())
                .send()
                .await
                .unwrap();
            let replications = client.list_replications().await.unwrap();
            assert_eq!(replications.len(), 1);
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_replication(
            #[future] client: ReductClient,
            settings: ReplicationSettings,
        ) {
            let client = client.await;
            client
                .create_replication("test-replication")
                .set_settings(settings.clone())
                .send()
                .await
                .unwrap();
            let replication = client.get_replication("test-replication").await.unwrap();
            assert_eq!(
                replication.info,
                ReplicationInfo {
                    name: "test-replication".to_string(),
                    is_active: false,
                    is_provisioned: false,
                    pending_records: 0,
                }
            );

            assert_eq!(
                replication.settings,
                ReplicationSettings {
                    dst_token: "***".to_string(),
                    ..settings
                }
            );
            assert_eq!(replication.diagnostics, Diagnostics::default());
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_replication(
            #[future] client: ReductClient,
            settings: ReplicationSettings,
        ) {
            let client = client.await;
            client
                .create_replication("test-replication")
                .set_settings(settings.clone())
                .send()
                .await
                .unwrap();
            let replication = client.get_replication("test-replication").await.unwrap();

            assert_eq!(
                replication.settings,
                ReplicationSettings {
                    dst_token: "***".to_string(),
                    ..settings
                }
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_delete_replication(
            #[future] client: ReductClient,
            settings: ReplicationSettings,
        ) {
            let client = client.await;
            client
                .create_replication("test-replication")
                .set_settings(settings.clone())
                .send()
                .await
                .unwrap();
            client.delete_replication("test-replication").await.unwrap();
            let replications = client.list_replications().await.unwrap();
            assert!(replications.is_empty());
        }

        #[fixture]
        fn settings() -> ReplicationSettings {
            ReplicationSettings {
                src_bucket: "test-bucket-1".to_string(),
                dst_bucket: "test-bucket-2".to_string(),
                dst_host: "http://127.0.0.1:8383".to_string(),
                dst_token: std::env::var("RS_API_TOKEN").unwrap_or("".to_string()),
                entries: vec![],
                include: Labels::default(),
                exclude: Labels::default(),
            }
        }
    }

    #[fixture]
    pub(crate) fn bucket_settings() -> BucketSettings {
        BucketSettings {
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(10_000_000_000),
            max_block_size: Some(512),
            max_block_records: Some(100),
        }
    }

    #[fixture]
    pub(crate) async fn client(bucket_settings: BucketSettings) -> ReductClient {
        let client = ReductClient::builder()
            .url("http://127.0.0.1:8383")
            .api_token(&std::env::var("RS_API_TOKEN").unwrap_or("".to_string()))
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

        for replication in client.list_replications().await.unwrap() {
            if replication.name.starts_with("test-replication") {
                client.delete_replication(&replication.name).await.unwrap();
            }
        }

        let bucket = client
            .create_bucket("test-bucket-1")
            .settings(bucket_settings.clone())
            .send()
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
            .create_bucket("test-bucket-2")
            .settings(bucket_settings)
            .send()
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
