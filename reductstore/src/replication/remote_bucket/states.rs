mod bucket_available;
mod bucket_unavailable;
mod initial_state;

use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use async_trait::async_trait;
pub(super) use initial_state::InitialState;
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;

/// A state of the remote bucket.

#[async_trait]
pub(super) trait RemoteBucketState {
    /// Write a record to the remote bucket.
    async fn write_batch(
        self: Box<Self>,
        entry_name: &str,
        records: Vec<(BoxedReadRecord, Transaction)>,
    ) -> Box<dyn RemoteBucketState + Sync + Send>;

    /// Is the bucket available?
    fn is_available(&self) -> bool;

    // Get the last result of the write operation.
    fn last_result(&self) -> &Result<ErrorRecordMap, ReductError>;
}
