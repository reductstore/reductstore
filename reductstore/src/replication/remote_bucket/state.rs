// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::bucket_available_state::BucketAvailableState;
use crate::replication::remote_bucket::client_wrapper::{
    BoxedBucketApi, BoxedClientApi, ReductBucketApi, ReductClientApi,
};
use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, TryStream};
use log::error;
use reduct_base::error::{IntEnum, ReductError};
use reduct_base::Labels;
use reduct_rs::{Bucket, ReductClient};
use std::pin::Pin;
use std::task::Poll;
use tokio::time::Instant;
use url::Url;

/// A state of the remote bucket.
#[async_trait]
pub(super) trait RemoteBucketState {
    /// Write a record to the remote bucket.
    async fn write_record(
        self: Box<Self>,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Box<dyn RemoteBucketState + Sync + Send>;

    /// Is the bucket available?
    fn ok(&self) -> bool;
}

pub(super) struct BucketUnavailableState {
    client: BoxedClientApi,
    bucket_name: String,
    init_time: Instant,
}

#[async_trait]
impl RemoteBucketState for BucketUnavailableState {
    async fn write_record(
        self: Box<Self>,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        if self.init_time.elapsed().as_secs() > 60 {
            let bucket = self.client.get_bucket(&self.bucket_name).await;
            return match bucket {
                Ok(bucket) => {
                    let new_state = Box::new(BucketAvailableState::new(self.client, bucket));
                    new_state
                        .write_record(entry, timestamp, labels, content_type, content_length, rx)
                        .await
                }
                Err(_) => {
                    error!(
                        "Failed to get remote bucket {}/{}",
                        self.client.url(),
                        self.bucket_name
                    );
                    Box::new(BucketUnavailableState::new(self.client, self.bucket_name))
                }
            };
        }

        self
    }

    fn ok(&self) -> bool {
        false
    }
}

impl BucketUnavailableState {
    pub fn new(client: BoxedClientApi, bucket_name: String) -> Self {
        Self {
            client,
            bucket_name,
            init_time: Instant::now(),
        }
    }
}
