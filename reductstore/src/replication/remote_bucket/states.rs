// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod bucket_available;
mod bucket_unavailable;
mod initial_state;

use crate::replication::remote_bucket::client_wrapper::{BoxedBucketApi, BoxedClientApi};
use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use async_trait::async_trait;
pub(super) use initial_state::InitialState;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::BoxedReadRecord;

async fn get_or_create_bucket(
    client: &BoxedClientApi,
    bucket_name: &str,
) -> Result<BoxedBucketApi, ReductError> {
    match client.get_bucket(bucket_name).await {
        Ok(bucket) => Ok(bucket),
        Err(err) if err.status() == ErrorCode::NotFound => {
            match client.create_bucket(bucket_name).await {
                Ok(bucket) => Ok(bucket),
                Err(err) if err.status() == ErrorCode::Conflict => {
                    client.get_bucket(bucket_name).await
                }
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

/// A state of the remote bucket.

#[async_trait]
pub(super) trait RemoteBucketState {
    /// Write a record to the remote bucket.
    async fn write_batch(
        self: Box<Self>,
        entry_name: &str,
        records: Vec<(BoxedReadRecord, Transaction)>,
    ) -> Box<dyn RemoteBucketState + Sync + Send>;

    /// Probe the remote bucket to check availability without writing.
    async fn probe(self: Box<Self>) -> Box<dyn RemoteBucketState + Sync + Send>;

    /// Is the bucket available?
    fn is_available(&self) -> bool;

    // Get the last result of the write operation.
    fn last_result(&self) -> &Result<ErrorRecordMap, ReductError>;
}
