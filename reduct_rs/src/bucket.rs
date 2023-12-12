// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use reqwest::Method;

use reduct_base::error::ErrorCode;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo, QuotaType};
use reduct_base::msg::entry_api::EntryInfo;

use crate::client::Result;
use crate::http_client::HttpClient;
use crate::record::query::QueryBuilder;
use crate::record::read_record::ReadRecordBuilder;
use crate::record::WriteRecordBuilder;
use crate::WriteBatchBuilder;

/// A bucket to store data in.
pub struct Bucket {
    pub(crate) name: String,
    pub(crate) http_client: Arc<HttpClient>,
}

pub struct BucketBuilder {
    name: String,
    exist_ok: bool,
    settings: BucketSettings,
    http_client: Arc<HttpClient>,
}

impl BucketBuilder {
    pub(crate) fn new(name: String, http_client: Arc<HttpClient>) -> Self {
        Self {
            name,
            exist_ok: false,
            settings: BucketSettings::default(),
            http_client,
        }
    }

    /// Don't fail if the bucket already exists.
    pub fn exist_ok(mut self, exist_ok: bool) -> Self {
        self.exist_ok = exist_ok;
        self
    }

    /// Set the quota type.
    pub fn quota_type(mut self, quota_type: QuotaType) -> Self {
        self.settings.quota_type = Some(quota_type);
        self
    }

    /// Set the quota size.
    pub fn quota_size(mut self, quota_size: u64) -> Self {
        self.settings.quota_size = Some(quota_size);
        self
    }

    /// Set the max block size.
    pub fn max_block_size(mut self, max_block_size: u64) -> Self {
        self.settings.max_block_size = Some(max_block_size);
        self
    }

    /// Set the max block records.
    pub fn max_block_records(mut self, max_block_records: u64) -> Self {
        self.settings.max_block_records = Some(max_block_records);
        self
    }

    /// Set and overwrite the settings of the bucket.
    pub fn settings(mut self, settings: BucketSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Create the bucket.
    pub async fn send(self) -> Result<Bucket> {
        let result = self
            .http_client
            .send_json(Method::POST, &format!("/b/{}", self.name), self.settings)
            .await;
        match result {
            Ok(_) => {}
            Err(e) => {
                if !(self.exist_ok && e.status() == ErrorCode::Conflict) {
                    return Err(e);
                }
            }
        }

        Ok(Bucket {
            name: self.name.clone(),
            http_client: Arc::clone(&self.http_client),
        })
    }
}

impl Bucket {
    /// Name of the bucket.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// URL of the server.
    pub fn server_url(&self) -> &str {
        &self.http_client.url()
    }

    /// Remove the bucket.
    ///
    /// # Returns
    ///
    /// Returns an error if the bucket could not be removed.
    pub async fn remove(&self) -> Result<()> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/b/{}", self.name));
        self.http_client.send_request(request).await?;
        Ok(())
    }

    /// Get the settings of the bucket.
    ///
    /// # Returns
    ///
    /// Return settings of the bucket
    pub async fn settings(&self) -> Result<BucketSettings> {
        Ok(self.full_info().await?.settings)
    }

    /// Set the settings of the bucket.
    ///
    /// # Arguments
    ///
    /// * `settings` - The new settings of the bucket.
    ///
    /// # Returns
    ///
    ///  Returns an error if the bucket could not be found.
    pub async fn set_settings(&self, settings: BucketSettings) -> Result<()> {
        self.http_client
            .send_json::<BucketSettings>(Method::PUT, &format!("/b/{}", self.name), settings)
            .await
    }

    /// Get full information about the bucket (stats, settings, entries).
    pub async fn full_info(&self) -> Result<FullBucketInfo> {
        self.http_client
            .send_and_receive_json::<(), FullBucketInfo>(
                Method::GET,
                &format!("/b/{}", self.name),
                None,
            )
            .await
    }

    /// Get bucket stats.
    pub async fn info(&self) -> Result<BucketInfo> {
        Ok(self.full_info().await?.info)
    }

    /// Get bucket entries.
    pub async fn entries(&self) -> Result<Vec<EntryInfo>> {
        Ok(self.full_info().await?.entries)
    }

    /// Create a record to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a record builder.
    pub fn write_record(&self, entry: &str) -> WriteRecordBuilder {
        WriteRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a batch to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a batch builder.
    pub fn write_batch(&self, entry: &str) -> WriteBatchBuilder {
        WriteBatchBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a record to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a record builder.
    pub fn read_record(&self, entry: &str) -> ReadRecordBuilder {
        ReadRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a record to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a record builder.
    ///
    /// ```no_run
    /// use reduct_rs::{ReductClient, ReductError};
    /// use std::time::SystemTime;
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///    let client = ReductClient::builder()
    ///         .url("https://play.reduct.store")
    ///         .api_token("reductstore")
    ///         .build();
    ///     let bucket = client.get_bucket("datasets").await?;
    ///     let query = bucket.query("cats").limit(10).send().await?;
    ///     tokio::pin!(query);
    ///     while let Some(record) = query.next().await {
    ///         let record = record?;
    ///         let content_ = record.bytes().await?;
    ///     }
    ///     Ok(())
    /// }
    ///  ```
    pub fn query(&self, entry: &str) -> QueryBuilder {
        QueryBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Remove an entry from the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove.
    ///
    /// # Returns
    ///
    /// Returns an error if the entry could not be removed.
    pub async fn remove_entry(&self, entry: &str) -> Result<()> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/b/{}/{}", self.name, entry));
        self.http_client.send_request(request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::{pin_mut, StreamExt};
    use rstest::{fixture, rstest};

    use reduct_base::error::ErrorCode;

    use crate::client::tests::{bucket_settings, client};
    use crate::client::ReductClient;
    use crate::record::RecordBuilder;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_bucket_full_info(#[future] bucket: Bucket) {
        let bucket = bucket.await;
        let FullBucketInfo {
            info,
            settings,
            entries,
        } = bucket.full_info().await.unwrap();
        assert_eq!(info, bucket.info().await.unwrap());
        assert_eq!(settings, bucket.settings().await.unwrap());
        assert_eq!(entries, bucket.entries().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_settings(#[future] bucket: Bucket, bucket_settings: BucketSettings) {
        let bucket = bucket.await;
        let settings = bucket.settings().await.unwrap();
        assert_eq!(settings, bucket_settings);

        let new_settings = BucketSettings {
            quota_size: Some(100),
            ..BucketSettings::default()
        };

        bucket.set_settings(new_settings.clone()).await.unwrap();
        assert_eq!(
            bucket.settings().await.unwrap(),
            BucketSettings {
                quota_size: new_settings.quota_size,
                ..bucket_settings
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_remove(#[future] bucket: Bucket) {
        let bucket = bucket.await;
        bucket.remove().await.unwrap();

        assert_eq!(
            bucket.info().await.err().unwrap().status,
            ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_write_record_data(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .data(Bytes::from("Hey"))
            .send()
            .await
            .unwrap();

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hey"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_write_record_stream(#[future] bucket: Bucket) {
        let chunks: Vec<Result<_>> = vec![Ok("hello"), Ok(" "), Ok("world")];

        let stream = futures_util::stream::iter(chunks);

        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .content_length(11)
            .stream(Box::pin(stream))
            .send()
            .await
            .unwrap();

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("hello world"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_record(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let record = bucket
            .read_record("entry-1")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.timestamp_us(), 1000);
        assert_eq!(record.content_length(), 12);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("bucket"), Some(&"1".to_string()));
        assert_eq!(record.labels().get("entry"), Some(&"1".to_string()));
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hey entry-1!"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_record_as_stream(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let record = bucket
            .read_record("entry-1")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        let mut stream = record.stream_bytes();
        assert_eq!(
            stream.next().await.unwrap(),
            Ok(Bytes::from("Hey entry-1!"))
        );
        assert_eq!(stream.next().await, None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_head_record(#[future] bucket: Bucket) {
        let record = bucket
            .await
            .read_record("entry-1")
            .timestamp_us(1000)
            .head_only(true)
            .send()
            .await
            .unwrap();

        assert_eq!(record.timestamp_us(), 1000);
        assert_eq!(record.content_length(), 12);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("bucket"), Some(&"1".to_string()));
        assert_eq!(record.labels().get("entry"), Some(&"1".to_string()));
    }

    mod query {
        use super::*;
        use chrono::Duration;
        use std::time::SystemTime;

        #[rstest]
        #[case(true, 10)]
        #[case(false, 10)]
        #[case(false, 10_000)]
        #[case(false, 30_000_000)]
        #[case(false, 100_000_000)]
        #[tokio::test]
        async fn test_query(
            #[future] bucket: Bucket,
            #[case] head_only: bool,
            #[case] size: usize,
        ) {
            let bucket: Bucket = bucket.await;
            let mut bodies: Vec<Vec<u8>> = Vec::new();
            for i in 0..3usize {
                let mut content = Vec::new();
                for _j in 0..size {
                    content.push(i as u8);
                }
                bodies.push(content);

                bucket
                    .write_record("entry-3")
                    .timestamp_us((i as u64) * 1_000_000)
                    .data(Bytes::from(bodies[i].clone()))
                    .send()
                    .await
                    .unwrap();
            }

            let query = bucket
                .query("entry-3")
                .ttl(Duration::minutes(1).to_std().unwrap())
                .head_only(head_only)
                .send()
                .await
                .unwrap();
            pin_mut!(query);
            let record = query.next().await.unwrap().unwrap();
            assert_eq!(record.timestamp_us(), 0);
            assert_eq!(record.content_length(), size);
            assert_eq!(record.content_type(), "application/octet-stream");

            if !head_only {
                assert_eq!(
                    record.bytes().await.unwrap(),
                    Bytes::from(bodies[0].clone())
                );
            }

            let record = query.next().await.unwrap().unwrap();
            assert_eq!(record.timestamp_us(), 1_000_000);
            assert_eq!(record.content_length(), size);
            assert_eq!(record.content_type(), "application/octet-stream");

            if !head_only {
                assert_eq!(
                    record.bytes().await.unwrap(),
                    Bytes::from(bodies[1].clone())
                );
            }

            let record = query.next().await.unwrap().unwrap();
            assert_eq!(record.timestamp_us(), 2_000_000);
            assert_eq!(record.content_length(), size);
            assert_eq!(record.content_type(), "application/octet-stream");

            if !head_only {
                assert_eq!(
                    record.bytes().await.unwrap(),
                    Bytes::from(bodies[2].clone())
                );
            }

            assert!(query.next().await.is_none());
        }

        #[rstest]
        #[tokio::test]
        async fn test_limit_query(#[future] bucket: Bucket) {
            let bucket: Bucket = bucket.await;
            let query = bucket.query("entry-1").limit(1).send().await.unwrap();

            pin_mut!(query);
            let _ = query.next().await.unwrap().unwrap();
            assert!(query.next().await.is_none());
        }

        #[rstest]
        #[tokio::test]
        async fn test_query_with_few_options(#[future] bucket: Bucket) {
            let bucket: Bucket = bucket.await;
            let query = bucket
                .query("entry-1")
                .start(SystemTime::now() - Duration::minutes(1).to_std().unwrap())
                .stop(SystemTime::now() + Duration::minutes(1).to_std().unwrap())
                .limit(1)
                .add_include("bucket", "1")
                .add_exclude("entry", "1")
                .send()
                .await;
            assert!(query.is_ok());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn remove_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket.remove_entry("entry-1").await.unwrap();
        assert!(
            bucket
                .read_record("entry-1")
                .send()
                .await
                .err()
                .unwrap()
                .status
                == ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_write(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let batch = bucket.write_batch("test");

        let record1 = RecordBuilder::new()
            .timestamp_us(1000)
            .data(Bytes::from("Hey,"))
            .add_label("test".into(), "1".into())
            .content_type("text/plain".into())
            .build();

        let stream = futures_util::stream::iter(vec![Ok(Bytes::from("World"))]);

        let record2 = RecordBuilder::new()
            .timestamp_us(2000)
            .stream(Box::pin(stream))
            .add_label("test".into(), "2".into())
            .content_type("text/plain".into())
            .content_length(5)
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();
        assert!(error_map.is_empty());

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.content_length(), 4);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("test"), Some(&"1".to_string()));
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hey,"));

        let record = bucket
            .read_record("test")
            .timestamp_us(2000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.content_length(), 5);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("test"), Some(&"2".to_string()));
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("World"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_write_with_error(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .data(Bytes::from("xxx"))
            .send()
            .await
            .unwrap();

        let batch = bucket.write_batch("test");
        let record1 = RecordBuilder::new()
            .timestamp_us(1000)
            .data(Bytes::from("Hey,"))
            .build();
        let record2 = RecordBuilder::new()
            .timestamp_us(2000)
            .data(Bytes::from("World"))
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();

        assert_eq!(error_map.len(), 1);
        assert_eq!(error_map.get(&1000).unwrap().status, ErrorCode::Conflict);
        assert_eq!(
            error_map.get(&1000).unwrap().message,
            "A record with timestamp 1000 already exists"
        );
    }

    #[fixture]
    async fn bucket(#[future] client: ReductClient) -> Bucket {
        client.await.get_bucket("test-bucket-1").await.unwrap()
    }
}
