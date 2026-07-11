// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::replication::remote_bucket::client_wrapper::{create_client, BoxedClientApi};
use crate::replication::remote_bucket::states::bucket_available::BucketAvailableState;
use crate::replication::remote_bucket::states::bucket_unavailable::BucketUnavailableState;
use crate::replication::remote_bucket::states::get_or_create_bucket;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::remote_bucket::RemoteBucketConfig;
use crate::replication::Transaction;
use async_trait::async_trait;
use log::error;
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
/// Initial state of the remote bucket.
pub(in crate::replication::remote_bucket) struct InitialState {
    client: BoxedClientApi,
    bucket_name: String,
    last_result: Result<ErrorRecordMap, ReductError>,
}

impl InitialState {
    pub fn new(config: &RemoteBucketConfig) -> Result<Self, ReductError> {
        let client = create_client(config)?;
        Ok(Self {
            client,
            bucket_name: config.bucket_name.clone(),
            last_result: Ok(ErrorRecordMap::new()),
        })
    }
}

#[async_trait]
impl RemoteBucketState for InitialState {
    async fn write_batch(
        self: Box<Self>,
        entry: &str,
        records: Vec<(BoxedReadRecord, Transaction)>,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        match get_or_create_bucket(&self.client, &self.bucket_name).await {
            Ok(bucket) => {
                // Bucket is available, transition to the available state and write the record.
                let new_state = Box::new(BucketAvailableState::new(self.client, bucket));
                new_state.write_batch(entry, records).await
            }
            Err(err) => {
                // Bucket is unavailable, transition to the unavailable state.
                error!(
                    "Failed to get remote bucket {}{}: {}",
                    self.client.url(),
                    self.bucket_name,
                    err
                );
                Box::new(BucketUnavailableState::new(
                    self.client,
                    self.bucket_name,
                    err,
                ))
            }
        }
    }

    async fn probe(self: Box<Self>) -> Box<dyn RemoteBucketState + Sync + Send> {
        match get_or_create_bucket(&self.client, &self.bucket_name).await {
            Ok(bucket) => Box::new(BucketAvailableState::new(self.client, bucket)),
            Err(err) => Box::new(BucketUnavailableState::new(
                self.client,
                self.bucket_name,
                err,
            )),
        }
    }

    fn is_available(&self) -> bool {
        false
    }
    fn last_result(&self) -> &Result<ErrorRecordMap, ReductError> {
        &self.last_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::remote_bucket::tests::{
        bucket, client, MockReductBucketApi, MockReductClientApi,
    };
    use mockall::{predicate, Sequence};
    use reduct_base::error::{ErrorCode, ReductError};
    use rstest::rstest;

    #[rstest]
    fn test_initial_state() {
        let state = InitialState::new(&RemoteBucketConfig {
            url: "http://localhost:8080".to_string(),
            bucket_name: "test_bucket".to_string(),
            api_token: "test_token".to_string(),
            verify_ssl: true,
            ca_path: None,
        })
        .unwrap();
        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_available(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_batch()
            .with(
                predicate::eq("test_entry"),
                predicate::always(), // TODO: check the records
            )
            .return_once(|_, _| Ok(ErrorRecordMap::new()));
        client
            .expect_get_bucket()
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
        assert_eq!(state.is_available(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_missing_created(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_batch()
            .with(predicate::eq("test_entry"), predicate::always())
            .return_once(|_, _| Ok(ErrorRecordMap::new()));
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Err(ReductError::not_found("bucket not found")));
        client
            .expect_create_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_created_concurrently(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_batch()
            .with(predicate::eq("test_entry"), predicate::always())
            .return_once(|_, _| Ok(ErrorRecordMap::new()));
        let mut sequence = Sequence::new();
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(move |_| Err(ReductError::not_found("bucket not found")));
        client
            .expect_create_bucket()
            .with(predicate::eq("test_bucket"))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(move |_| Err(ReductError::new(ErrorCode::Conflict, "already exists")));
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_missing_create_fails(mut client: MockReductClientApi) {
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Err(ReductError::not_found("bucket not found")));
        client
            .expect_create_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Err(ReductError::forbidden("denied")));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(state.last_result(), &Err(ReductError::forbidden("denied")));
        assert!(!state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_unavailable(mut client: MockReductClientApi) {
        client
            .expect_get_bucket()
            .return_once(move |_| Err(ReductError::bad_request("test error")));
        client.expect_create_bucket().times(0);

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(
            state.last_result(),
            &Err(ReductError::bad_request("test error"))
        );
        assert_eq!(state.is_available(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_probe_available(mut client: MockReductClientApi, bucket: MockReductBucketApi) {
        client
            .expect_get_bucket()
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.probe().await;
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_probe_unavailable(mut client: MockReductClientApi) {
        client
            .expect_get_bucket()
            .return_once(move |_| Err(ReductError::bad_request("test error")));
        client.expect_create_bucket().times(0);

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.probe().await;
        assert!(!state.is_available());
    }
}
