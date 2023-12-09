// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{BoxedBucketApi, BoxedClientApi};
use crate::replication::remote_bucket::states::bucket_unavailable::BucketUnavailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use log::error;
use reduct_base::error::IntEnum;
use reduct_base::Labels;

/// A state when the remote bucket is available.
pub(in crate::replication::remote_bucket) struct BucketAvailableState {
    client: BoxedClientApi,
    bucket: BoxedBucketApi,
}

impl BucketAvailableState {
    pub fn new(client: BoxedClientApi, bucket: BoxedBucketApi) -> Self {
        Self { client, bucket }
    }
}

#[async_trait]
impl RemoteBucketState for BucketAvailableState {
    async fn write_record(
        self: Box<Self>,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        match self
            .bucket
            .write_record(entry, timestamp, labels, content_type, content_length, rx)
            .await
        {
            Ok(_) => self,
            Err(err) => {
                error!(
                    "Failed to write record to remote bucket {}/{}: {}",
                    self.bucket.server_url(),
                    self.bucket.name(),
                    err
                );

                // if it is a network error, we can retry
                if err.status.int_value() < 0 {
                    Box::new(BucketUnavailableState::new(
                        self.client,
                        self.bucket.name().to_string(),
                    ))
                } else {
                    self
                }
            }
        }
    }

    fn ok(&self) -> bool {
        true
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
    async fn write_record_ok(client: MockReductClientApi, mut bucket: MockReductBucketApi) {
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
            .returning(|_, _, _, _, _, _| Ok(()));

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let (_, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test_entry", 0, Labels::new(), "text/plain", 0, rx)
            .await;
        assert!(state.ok());
    }

    #[rstest]
    #[tokio::test]
    async fn write_record_conn_err(client: MockReductClientApi, mut bucket: MockReductBucketApi) {
        bucket
            .expect_write_record()
            .returning(|_, _, _, _, _, _| Err(ReductError::new(ErrorCode::Timeout, "")));

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let (_, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test", 0, Labels::new(), "text/plain", 0, rx)
            .await;
        assert_eq!(state.ok(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn write_record_server_err(client: MockReductClientApi, mut bucket: MockReductBucketApi) {
        bucket.expect_write_record().returning(|_, _, _, _, _, _| {
            Err(ReductError::new(ErrorCode::InternalServerError, ""))
        });

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let (_, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test", 0, Labels::new(), "text/plain", 0, rx)
            .await;
        assert_eq!(state.ok(), true);
    }
}
