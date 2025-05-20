use crate::error::ReductError;
use crate::{internal_server_error, Labels};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tokio::runtime::Handle;

pub type WriteChunk = Result<Option<Bytes>, ReductError>;
pub type ReadChunk = Option<Result<Bytes, ReductError>>;

#[derive(Debug, Clone, PartialEq)]
pub struct RecordMeta {
    timestamp: u64,
    state: i32,
    labels: Labels,
    content_type: String,
    content_length: u64,
    computed_labels: Labels,
    last: bool,
}

pub struct BuilderRecordMeta {
    timestamp: u64,
    state: i32,
    labels: Labels,
    content_type: String,
    content_length: u64,
    computed_labels: Labels,
    last: bool,
}

impl BuilderRecordMeta {
    pub fn timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn state(mut self, state: i32) -> Self {
        self.state = state;
        self
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn content_type(mut self, content_type: String) -> Self {
        self.content_type = content_type;
        self
    }

    pub fn content_length(mut self, content_length: u64) -> Self {
        self.content_length = content_length;
        self
    }

    pub fn computed_labels(mut self, computed_labels: Labels) -> Self {
        self.computed_labels = computed_labels;
        self
    }

    pub fn last(mut self, last: bool) -> Self {
        self.last = last;
        self
    }
    pub fn build(self) -> RecordMeta {
        RecordMeta {
            timestamp: self.timestamp,
            state: self.state,
            labels: self.labels,
            content_type: self.content_type,
            content_length: self.content_length,
            computed_labels: self.computed_labels,
            last: self.last,
        }
    }
}

impl RecordMeta {
    pub fn builder() -> BuilderRecordMeta {
        BuilderRecordMeta {
            timestamp: 0,
            state: 0,
            labels: Labels::new(),
            content_type: "application/octet-stream".to_string(),
            content_length: 0,
            computed_labels: Labels::new(),
            last: false,
        }
    }

    /// Returns the timestamp of the record as Unix time in microseconds.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the labels associated with the record.
    pub fn labels(&self) -> &Labels {
        &self.labels
    }

    /// For filtering unfinished records.
    pub fn state(&self) -> i32 {
        self.state
    }

    /// Returns true if this is the last record in the stream.
    pub fn last(&self) -> bool {
        self.last
    }

    /// Returns computed labels associated with the record.
    ///
    /// Computed labels are labels that are added by query processing and are not part of the original record.
    pub fn computed_labels(&self) -> &Labels {
        &self.computed_labels
    }

    /// Returns the labels associated with the record.
    ///
    /// Computed labels are labels that are added by query processing and are not part of the original record.
    pub fn computed_labels_mut(&mut self) -> &mut Labels {
        &mut self.computed_labels
    }

    /// Returns the length of the record content in bytes.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the content type of the record as a MIME type.
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    pub fn set_last(&mut self, last: bool) {
        self.last = last;
    }
}

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

    /// Returns meta information about the record.
    fn meta(&self) -> &RecordMeta;
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use log::Metadata;
    use rstest::rstest;

    use tokio::task::spawn_blocking;

    #[rstest]
    #[tokio::test]
    async fn test_blocking_read() {
        let result = spawn_blocking(move || {
            let mut record = MockRecord::new();
            record.blocking_read()
        });
        assert_eq!(
            result.await.unwrap().unwrap(),
            Ok(Bytes::from_static(b"test"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_default_read_timeout() {
        let mut record = MockRecord::new();
        let result = record.read_timeout(Duration::from_secs(1)).await;
        assert_eq!(result.unwrap(), Ok(Bytes::from_static(b"test")));

        let result = record.read_timeout(Duration::from_millis(5)).await;
        assert_eq!(
            result.unwrap().err().unwrap(),
            internal_server_error!("Timeout reading record: deadline has elapsed")
        );
    }

    pub struct MockRecord {
        metadata: RecordMeta,
    }

    impl MockRecord {
        pub fn new() -> Self {
            Self {
                metadata: RecordMeta::builder()
                    .timestamp(0)
                    .state(0)
                    .labels(Labels::new())
                    .content_type("application/octet-stream".to_string())
                    .content_length(0)
                    .computed_labels(Labels::new())
                    .last(false)
                    .build(),
            }
        }
    }

    #[async_trait]
    impl ReadRecord for MockRecord {
        async fn read(&mut self) -> ReadChunk {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Some(Ok(Bytes::from("test")))
        }

        fn meta(&self) -> &RecordMeta {
            &self.metadata
        }
    }
}
