mod bucket_available;
mod bucket_unavailable;
mod initial_state;

use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use reduct_base::Labels;

pub(super) use initial_state::InitialState;
use reduct_base::error::ReductError;

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
    fn is_available(&self) -> bool;

    // Get the last result of the write operation.
    fn last_result(&self) -> &Result<(), ReductError>;
}
