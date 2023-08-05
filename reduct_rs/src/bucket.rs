// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use crate::http_client::HttpClient;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo};
use reqwest::Method;

use crate::record::query::QueryBuilder;
use crate::record::read_record::ReadRecordBuilder;
use crate::record::WriteRecordBuilder;
use reduct_base::msg::entry_api::EntryInfo;
use std::sync::Arc;

/// A bucket to store data in.
pub struct Bucket {
    pub(crate) name: String,
    pub(crate) http_client: Arc<HttpClient>,
}

impl Bucket {
    /// Name of the bucket.
    pub fn name(&self) -> &str {
        &self.name
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
    pub fn write_record(&self, entry: &str) -> WriteRecordBuilder {
        WriteRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a record to write to the bucket.
    pub fn read_record(&self, entry: &str) -> ReadRecordBuilder {
        ReadRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a record to write to the bucket.
    pub fn query(&self, entry: &str) -> QueryBuilder {
        QueryBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, Bytes};
    use futures_util::{pin_mut, StreamExt};

    use crate::client::tests::{bucket_settings, client};
    use crate::client::ReductClient;

    use crate::record::Record;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

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
        let record: Record = bucket
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

    #[rstest]
    #[tokio::test]
    async fn test_query(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let mut query = bucket.query("entry-1").send().await.unwrap();

        pin_mut!(query);
        let record = query.next().await.unwrap().unwrap();
        assert_eq!(record.timestamp_us(), 1000);
    }

    #[fixture]
    async fn bucket(#[future] client: ReductClient) -> Bucket {
        client.await.get_bucket("test-bucket-1").await.unwrap()
    }
}
