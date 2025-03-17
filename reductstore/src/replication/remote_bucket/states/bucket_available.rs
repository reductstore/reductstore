// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{BoxedBucketApi, BoxedClientApi};
use crate::replication::remote_bucket::states::bucket_unavailable::BucketUnavailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use crate::storage::entry::RecordReader;
use log::{debug, error, warn};
use reduct_base::error::ErrorCode::MethodNotAllowed;
use reduct_base::error::{IntEnum, ReductError};
use reduct_base::io::ReadRecord;
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
                "Failed to write record to remote bucket {}{}: {}",
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

impl RemoteBucketState for BucketAvailableState {
    fn write_batch(
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
            match self.bucket.update_batch(entry_name, &records_to_update) {
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
            match self.bucket.write_batch(entry_name, records_to_write) {
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

    fn is_available(&self) -> bool {
        true
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
    use crate::storage::proto::{us_to_ts, Record};
    use mockall::predicate;

    use crate::storage::entry::RecordReader;
    use reduct_base::error::{ErrorCode, ReductError};
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_write_record_ok(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_write: (RecordReader, Transaction),
    ) {
        bucket
            .expect_write_batch()
            .with(
                predicate::eq("test_entry"),
                predicate::always(), // TODO: check the records
            )
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        bucket.expect_update_batch().times(0);

        let state = Box::new(BucketAvailableState {
            client: Box::new(client),
            bucket: Box::new(bucket),
            last_result: Err(ReductError::new(ErrorCode::Timeout, "")), // to check that it is reset
        });

        let state = state.write_batch("test_entry", vec![record_to_write]);
        assert!(state.last_result().is_ok());
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_record_ok(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_update: (RecordReader, Transaction),
    ) {
        bucket.expect_write_batch().times(0);
        bucket
            .expect_update_batch()
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

        let state = state.write_batch("test_entry", vec![record_to_update]);
        assert!(state.last_result().is_ok());
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_record_conn_err(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_write: (RecordReader, Transaction),
    ) {
        bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "")));
        bucket.expect_update_batch().times(0);

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record_to_write]);
        assert_eq!(
            state.last_result(),
            &Err(ReductError::new(ErrorCode::Timeout, ""))
        );
        assert!(!state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_record_conn_err(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_update: (RecordReader, Transaction),
    ) {
        bucket.expect_write_batch().times(0);
        bucket
            .expect_update_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "")));

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record_to_update]);
        assert_eq!(
            state.last_result(),
            &Err(ReductError::new(ErrorCode::Timeout, ""))
        );
        assert!(!state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_record_server_err(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_write: (RecordReader, Transaction),
    ) {
        bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::InternalServerError, "")));

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record_to_write]);
        assert_eq!(
            state.last_result(),
            &Err(ReductError::new(ErrorCode::InternalServerError, ""))
        );
        assert!(state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_record_record_errors(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_write: (RecordReader, Transaction),
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

        let state = state.write_batch("test", vec![record_to_write]);
        let error_map = state.last_result().as_ref().unwrap();

        assert_eq!(error_map.len(), 1);
        assert_eq!(error_map.get(&1).unwrap().status, ErrorCode::Conflict);
        assert_eq!(error_map.get(&1).unwrap().message, "AlreadyExists");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_record_record_errors(
        client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
        record_to_update: (RecordReader, Transaction),
    ) {
        bucket.expect_update_batch().returning(|_, records| {
            assert_eq!(records.len(), 1);
            Ok(ErrorRecordMap::from_iter(vec![(
                0u64,
                ReductError::new(ErrorCode::NotFound, "Not found"),
            )]))
        });
        bucket.expect_write_batch().returning(|_, records| {
            assert_eq!(records.len(), 2, "we write the new record and failed one");
            Ok(ErrorRecordMap::new())
        });

        let state = Box::new(BucketAvailableState::new(
            Box::new(client),
            Box::new(bucket),
        ));

        let state = state.write_batch("test", vec![record_to_update, record_to_write()]);
        assert!(
            state.last_result().is_ok(),
            "we should not have any errors because wrote error records"
        );
        assert!(state.is_available());
    }

    #[fixture]
    fn record_to_write() -> (RecordReader, Transaction) {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        (
            RecordReader::form_record_with_rx(
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

    #[fixture]
    fn record_to_update() -> (RecordReader, Transaction) {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        (
            RecordReader::form_record_with_rx(
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
            Transaction::UpdateRecord(0),
        )
    }
}
