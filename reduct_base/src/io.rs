use crate::error::ReductError;
use crate::{internal_server_error, Labels};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tokio::runtime::Handle;

pub type WriteChunk = Result<Option<Bytes>, ReductError>;
pub type ReadChunk = Option<Result<Bytes, ReductError>>;

/// Represents a record in the storage engine that can be read as a stream of bytes.
#[async_trait]
pub trait ReadRecord {
    /// Reads a chunk of the record content.
    ///
    /// # Returns
    ///
    /// A chunk of the record content. If the chunk is an error or None, the reader should stop.
    async fn read(&mut self) -> ReadChunk;

    /// Reads a chunk of the record content with a timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum time to wait for the next chunk.
    ///
    /// # Returns
    ///
    /// A chunk of the record content. If the chunk is an error or None, the reader should stop.
    async fn read_timeout(&mut self, timeout: Duration) -> ReadChunk {
        match tokio::time::timeout(timeout, self.read()).await {
            Ok(chunk) => chunk,
            Err(er) => Some(Err(internal_server_error!(
                "Timeout reading record: {}",
                er
            ))),
        }
    }

    /// Reads a chunk of the record content synchronously.
    fn blocking_read(&mut self) -> ReadChunk {
        Handle::current().block_on(self.read())
    }

    /// Returns the timestamp of the record as Unix time in microseconds.
    fn timestamp(&self) -> u64;

    /// Returns the length of the record content in bytes.
    fn content_length(&self) -> u64;

    /// Returns the content type of the record as a MIME type.
    fn content_type(&self) -> &str;

    /// Returns true if this is the last record in the stream.
    fn last(&self) -> bool;

    /// Returns the labels associated with the record.
    fn labels(&self) -> &Labels;

    /// Returns computed labels associated with the record.
    ///
    /// Computed labels are labels that are added by query processing and are not part of the original record.
    fn computed_labels(&self) -> &Labels;

    /// Returns the labels associated with the record.
    ///
    /// Computed labels are labels that are added by query processing and are not part of the original record.
    fn computed_labels_mut(&mut self) -> &mut Labels;
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
