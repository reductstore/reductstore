use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, TryStream};
use log::error;
use reduct_base::error::{IntEnum, ReductError};
use reduct_base::Labels;
use reduct_rs::{Bucket, ReductClient};
use std::pin::Pin;
use std::task::Poll;
use tokio::sync::mpsc::Receiver;
use url::Url;

#[async_trait]
pub(super) trait RemoteBucketState {
    async fn write_record(
        self,
        entry: String,
        timestamp: u64,
        labels: Labels,
        content_type: String,
        content_length: u64,
        rx: RecordRx,
    ) -> Option<Box<dyn RemoteBucketState>>;

    fn ok(&self) -> bool;
}

pub(super) struct InitialState {
    client: ReductClient,
    bucket_name: String,
}

impl InitialState {
    pub fn new(url: Url, bucket: &str, api_token: &str) -> Self {
        let client = ReductClient::builder()
            .url(url.as_str())
            .api_token(api_token)
            .build();

        Self {
            client,
            bucket_name: bucket.to_string(),
        }
    }
}

#[async_trait]
impl RemoteBucketState for InitialState {
    async fn write_record(
        self,
        entry: String,
        timestamp: u64,
        labels: Labels,
        content_type: String,
        content_length: u64,
        rx: RecordRx,
    ) -> Option<Box<dyn RemoteBucketState>> {
        let bucket = self.client.get_bucket(&self.bucket_name).await;
        match bucket {
            Ok(bucket) => {
                let new_state = BucketAvailableState {
                    client: self.client,
                    bucket,
                };
                new_state
                    .write_record(entry, timestamp, labels, content_type, content_length, rx)
                    .await
            }
            Err(_) => Some(Box::new(BucketUnavailableState {
                client: self.client,
                bucket_name: self.bucket_name,
            })),
        }
    }

    fn ok(&self) -> bool {
        false
    }
}

struct BucketAvailableState {
    client: ReductClient,
    bucket: Bucket,
}

#[async_trait]
impl RemoteBucketState for BucketAvailableState {
    async fn write_record(
        self,
        entry: String,
        timestamp: u64,
        labels: Labels,
        content_type: String,
        content_length: u64,
        rx: RecordRx,
    ) -> Option<Box<dyn RemoteBucketState>> {
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

        let writer = self
            .bucket
            .write_record(entry.as_str())
            .timestamp_us(timestamp)
            .labels(labels)
            .content_type(content_type.as_str())
            .content_length(content_length)
            .stream(RxWrapper { rx });
        match writer.send().await {
            Ok(_) => None,
            Err(err) => {
                error!(
                    "Failed to write record to remote bucket {}/{}: {}",
                    self.bucket.server_url(),
                    self.bucket.name(),
                    err
                );

                // if it is a network error, we can retry
                if err.status.int_value() < 0 {
                    Some(Box::new(BucketUnavailableState {
                        client: self.client,
                        bucket_name: self.bucket.name().to_string(),
                    }))
                } else {
                    None
                }
            }
        }
    }

    fn ok(&self) -> bool {
        true
    }
}

struct BucketUnavailableState {
    client: ReductClient,
    bucket_name: String,
}

#[async_trait]
impl RemoteBucketState for BucketUnavailableState {
    async fn write_record(
        self,
        entry: String,
        timestamp: u64,
        labels: Labels,
        content_type: String,
        content_length: u64,
        rx: RecordRx,
    ) -> Option<Box<dyn RemoteBucketState>> {
        None
    }

    fn ok(&self) -> bool {
        false
    }
}
