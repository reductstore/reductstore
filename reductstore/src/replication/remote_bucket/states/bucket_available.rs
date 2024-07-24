// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{BoxedBucketApi, BoxedClientApi};
use crate::replication::remote_bucket::states::bucket_unavailable::BucketUnavailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use crate::storage::bucket::RecordReader;
use async_trait::async_trait;
use hyper::HeaderMap;
use log::{debug, error, warn};
use reduct_base::error::ErrorCode::MethodNotAllowed;
use reduct_base::error::{ErrorCode, IntEnum, ReductError};
use std::collections::BTreeMap;

/// A state when the remote bucket is available.
pub(in crate::replication::remote_bucket) struct BucketAvailableState {
    client: BoxedClientApi,
    bucket: BoxedBucketApi,
    last_result: Result<ErrorRecordMap, ReductError>,
}

impl BucketAvailableState {
    pub fn new(client: BoxedClientApi, bucket: BoxedBucketApi) -> Self {
        Self {
            client,
            bucket,
            last_result: Ok(ErrorRecordMap::new()),
        }
    }

    fn check_error_and_change_state(
        mut self: Box<Self>,
        err: ReductError,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        // if it is a network error, we can retry got to unavailable state and wait

        if err.status.int_value() < 0 {
            error!(
                "Failed to write record to remote bucket {}/{}: {}",
                self.bucket.server_url(),
                self.bucket.name(),
                err
            );
            Box::new(BucketUnavailableState::new(
                self.client,
                self.bucket.name().to_string(),
                err,
            ))
        } else {
            self.last_result = Err(err);
            self
        }
    }
}

#[async_trait]
impl RemoteBucketState for BucketAvailableState {
    async fn write_batch(
        mut self: Box<Self>,
        entry_name: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        let mut records_to_update = Vec::new();
        let mut records_to_write = Vec::new();
        for (record, transaction) in records {
            match transaction {
                Transaction::WriteRecord(_) => {
                    records_to_write.push(record);
                }
                Transaction::UpdateRecord(_) => {
                    records_to_update.push(record);
                }
            }
        }

        let error_map = if !records_to_update.is_empty() {
            match self
                .bucket
                .update_batch(entry_name, &records_to_update)
                .await
            {
                Ok(error_map) => {
                    // all good keep the state
                    error_map
                }
                Err(err) => {
                    debug!(
                        "Failed to update records to remote bucket {}/{}: {}",
                        self.bucket.server_url(),
                        self.bucket.name(),
                        err
                    );

                    if err.status != MethodNotAllowed {
                        return self.check_error_and_change_state(err);
                    }

                    warn!("Please update the remote instance up to 1.11: {}", err);

                    let mut error_map = BTreeMap::new();
                    for record in &records_to_update {
                        error_map.insert(record.timestamp(), err.clone());
                    }
                    error_map
                }
            }
        } else {
            BTreeMap::new()
        };

        // Write the records that failed to update with new records.
        while let Some(record) = records_to_update.pop() {
            if error_map.contains_key(&record.timestamp()) {
                records_to_write.push(record);
            }
        }

        if !records_to_write.is_empty() {
            match self.bucket.write_batch(entry_name, records_to_write).await {
                Ok(error_map) => {
                    self.last_result = Ok(error_map);
                    self
                }
                Err(err) => {
                    debug!(
                        "Failed to write record to remote bucket {}/{}: {}",
                        self.bucket.server_url(),
                        self.bucket.name(),
                        err
                    );

                    self.check_error_and_change_state(err)
                }
            }
        } else {
            self.last_result = Ok(error_map);
            self
        }
    }

    fn last_result(&self) -> &Result<ErrorRecordMap, ReductError> {
        &self.last_result
    }

    fn is_available(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::remote_bucket::tests::{
        bucket, client, MockReductBucketApi, MockReductClientApi,
    };
    use crate::storage::proto::{us_to_ts, Record};
    use mockall::predicate;

    use reduct_base::error::{ErrorCode, ReductError};
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn write_record_ok(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record: (RecordReader, Transaction),
    ) {
        bucket
            .expect_write_batch()
            .with(
                predicate::eq("test_entry"),
                predicate::always(), // TODO: check the records
            )
            .returning(|_, _| Ok(ErrorRecordMap::new()));

        let state = Box::new(BucketAvailableState {
            client: Box::new(client),
            bucket: Box::new(bucket),
            last_result: Err(ReductError::new(ErrorCode::Timeout, "")), // to check that it is reset
        });

        let state = state.write_batch("test_entry", vec![record]).await;
        assert!(state.last_result().is_ok());
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn write_record_conn_err(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record: (RecordReader, Transaction),
    ) {
        bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "")));

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record]).await;
        assert_eq!(
            state.last_result(),
            &Err(ReductError::new(ErrorCode::Timeout, ""))
        );
        assert!(!state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn write_record_server_err(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record: (RecordReader, Transaction),
    ) {
        bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::InternalServerError, "")));

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record]).await;
        assert_eq!(
            state.last_result(),
            &Err(ReductError::new(ErrorCode::InternalServerError, ""))
        );
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn write_record_record_errors(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record: (RecordReader, Transaction),
    ) {
        bucket.expect_write_batch().returning(|_, _| {
            Ok(ErrorRecordMap::from_iter(vec![(
                1u64,
                ReductError::new(ErrorCode::Conflict, "AlreadyExists"),
            )]))
        });

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record]).await;
        let error_map = state.last_result().as_ref().unwrap();

        assert_eq!(error_map.len(), 1);
        assert_eq!(error_map.get(&1).unwrap().status, ErrorCode::Conflict);
        assert_eq!(error_map.get(&1).unwrap().message, "AlreadyExists");
    }

    #[fixture]
    fn record() -> (RecordReader, Transaction) {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        (
            RecordReader::new(
                rx,
                Record {
                    timestamp: Some(us_to_ts(&0)),
                    labels: Vec::new(),
                    begin: 0,
                    end: 0,
                    content_type: "text/plain".to_string(),
                    state: 0,
                },
                false,
            ),
            Transaction::WriteRecord(0),
        )
    }
}
