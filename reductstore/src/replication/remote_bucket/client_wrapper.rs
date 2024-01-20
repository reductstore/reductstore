// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use reduct_rs::{Bucket, ReductClient};
use std::pin::Pin;
use std::task::Poll;

// A wrapper around the Reduct client API to make it easier to mock.
#[async_trait]
pub(super) trait ReductClientApi {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError>;

    fn url(&self) -> &str;
}

pub(super) type BoxedClientApi = Box<dyn ReductClientApi + Sync + Send>;

// A wrapper around the Reduct bucket API to make it easier to mock.
#[async_trait]
pub(super) trait ReductBucketApi {
    async fn write_record(
        &self,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Result<(), ReductError>;

    fn server_url(&self) -> &str;

    fn name(&self) -> &str;
}

pub(super) type BoxedBucketApi = Box<dyn ReductBucketApi + Sync + Send>;

struct ReductClientWrapper {
    client: ReductClient,
}

impl ReductClientWrapper {
    fn new(url: &str, api_token: &str) -> Self {
        let client = ReductClient::builder()
            .url(url)
            .api_token(api_token)
            .timeout(std::time::Duration::from_secs(1))
            .http1_only()
            .build();

        Self { client }
    }
}

struct BucketWrapper {
    bucket: Bucket,
}

impl BucketWrapper {
    fn new(bucket: Bucket) -> Self {
        Self { bucket }
    }
}

#[async_trait]
impl ReductClientApi for ReductClientWrapper {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError> {
        let bucket = self.client.get_bucket(bucket_name).await?;
        Ok(Box::new(BucketWrapper::new(bucket)))
    }

    fn url(&self) -> &str {
        self.client.url()
    }
}

#[async_trait]
impl ReductBucketApi for BucketWrapper {
    async fn write_record(
        &self,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Result<(), ReductError> {
        struct RxWrapper {
            rx: RecordRx,
        }

        impl Stream for RxWrapper {
            type Item = Result<Bytes, ReductError>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.rx.poll_recv(cx)
            }
        }

        self.bucket
            .write_record(entry)
            .timestamp_us(timestamp)
            .labels(labels)
            .content_type(content_type)
            .content_length(content_length)
            .stream(RxWrapper { rx: rx })
            .send()
            .await
    }

    fn server_url(&self) -> &str {
        self.bucket.server_url()
    }

    fn name(&self) -> &str {
        self.bucket.name()
    }
}

/// Create a new Reduct client wrapper.
pub(super) fn create_client(url: &str, api_token: &str) -> BoxedClientApi {
    Box::new(ReductClientWrapper::new(url, api_token))
}
