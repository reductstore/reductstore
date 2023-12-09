// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{
    BoxedBucketApi, BoxedClientApi, ReductBucketApi, ReductClientApi,
};
use crate::replication::remote_bucket::states::bucket_available::BucketAvailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, TryStream};
use log::error;
use reduct_base::Labels;
use reduct_rs::{Bucket, ReductClient};
use std::pin::Pin;
use std::task::Poll;
use tokio::time::{Duration, Instant};
use url::Url;

pub(in crate::replication::remote_bucket) struct BucketUnavailableState {
    client: BoxedClientApi,
    bucket_name: String,
    init_time: Instant,
    timeout: Duration,
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
        if self.init_time.elapsed() > self.timeout {
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
            timeout: Duration::new(60, 0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::remote_bucket::tests::{
        bucket, client, MockReductBucketApi, MockReductClientApi,
    };
    use mockall::predicate;
    use reduct_base::error::{ErrorCode, ReductError};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_write_record_timeout(mut client: MockReductClientApi) {
        client.expect_get_bucket().times(0);

        let client = Box::new(client);
        let bucket_name = "bucket".to_string();
        let state = Box::new(BucketUnavailableState::new(client, bucket_name));

        let (_, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("entry", 1234, Labels::new(), "text/plain", 5, rx)
            .await;
        assert_eq!(state.ok(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_err(mut client: MockReductClientApi) {
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Err(ReductError::not_found("")));

        let state = state_without_timeout(client);
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test_entry", 0, Labels::new(), "text/plain", 0, rx)
            .await;
        assert_eq!(state.ok(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_record_ok(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_record()
            .with(
                predicate::eq("test_entry"),
                predicate::eq(0),
                predicate::eq(Labels::new()),
                predicate::eq("text/plain"),
                predicate::eq(0),
                predicate::always(),
            )
            .return_once(|_, _, _, _, _, _| Ok(()));
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = state_without_timeout(client);
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test_entry", 0, Labels::new(), "text/plain", 0, rx)
            .await;
        assert!(state.ok());
    }

    fn state_without_timeout(client: MockReductClientApi) -> Box<BucketUnavailableState> {
        Box::new(BucketUnavailableState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            init_time: Instant::now() - Duration::new(61, 0),
            timeout: Duration::new(0, 0),
        })
    }
}
