use crate::error::ReductError;
use crate::Labels;
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

pub type WriteChunk = Result<Option<Bytes>, ReductError>;
pub type ReadChunk = Option<Result<Bytes, ReductError>>;

/// Represents a record that can be read as a stream of bytes.
#[async_trait]
pub trait ReadRecord {
    async fn read(&mut self) -> ReadChunk;

    async fn read_timeout(&mut self, timeout: Duration) -> ReadChunk;

    fn blocking_read(&mut self) -> ReadChunk;

    fn timestamp(&self) -> u64;

    fn content_length(&self) -> u64;

    fn content_type(&self) -> &str;
    fn last(&self) -> bool;

    fn labels(&self) -> &Labels;
}

#[async_trait]
pub trait WriteRecord {
    /// Sends a chunk of the record content.
    ///
    /// Stops the writer if the chunk is an error or None.
    async fn send(&mut self, chunk: WriteChunk) -> Result<(), ReductError>;

    fn blocking_send(&mut self, chunk: WriteChunk) -> Result<(), ReductError>;

    async fn send_timeout(
        &mut self,
        chunk: WriteChunk,
        timeout: Duration,
    ) -> Result<(), ReductError>;
}
