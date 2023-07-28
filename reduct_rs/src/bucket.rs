// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use crate::http_client::HttpClient;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo};
use reqwest::Method;

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
        self.http_client.delete(&format!("/b/{}", self.name)).await
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::client::tests::{bucket_settings, client};
    use crate::client::ReductClient;
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

    #[fixture]
    async fn bucket(#[future] client: ReductClient) -> Bucket {
        client.await.get_bucket("test-bucket-1").await.unwrap()
    }
}
