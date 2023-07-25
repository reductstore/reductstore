// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use crate::http_client::HttpClient;
use reduct_base::msg::bucket_api::{BucketSettings, FullBucketInfo};
use reqwest::Method;
use std::fmt::format;
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
        self.http_client
            .request_json::<(), BucketSettings>(Method::GET, &format!("/b/{}", self.name), None)
            .await
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
            .request_json::<BucketSettings, ()>(
                Method::PUT,
                &format!("/b/{}", self.name),
                Some(settings),
            )
            .await
    }

    /// Get full information about the bucket.
    pub async fn info(&self) -> Result<FullBucketInfo> {
        self.http_client
            .request_json::<(), FullBucketInfo>(
                Method::GET,
                &format!("/b/{}/info", self.name),
                None,
            )
            .await
    }
}
