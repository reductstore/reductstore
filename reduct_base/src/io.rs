use crate::error::ReductError;
use crate::{internal_server_error, Labels};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
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
}

pub struct BuilderRecordMeta {
    timestamp: u64,
    state: i32,
    labels: Labels,
    content_type: String,
    content_length: u64,
    computed_labels: Labels,
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

    pub fn labels<T: ToString>(mut self, labels: HashMap<T, T>) -> Self {
        self.labels = labels
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
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

    pub fn computed_labels<T: ToString>(mut self, computed_labels: HashMap<T, T>) -> Self {
        self.computed_labels = computed_labels
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self
    }

    /// Builds a `RecordMeta` instance from the builder.
    pub fn build(self) -> RecordMeta {
        RecordMeta {
            timestamp: self.timestamp,
            state: self.state,
            labels: self.labels,
            content_type: self.content_type,
            content_length: self.content_length,
            computed_labels: self.computed_labels,
        }
    }
}

impl RecordMeta {
    /// Creates a builder for a new `RecordMeta` instance.
    pub fn builder() -> BuilderRecordMeta {
        BuilderRecordMeta {
            timestamp: 0,
            state: 0,
            labels: Labels::new(),
            content_type: "application/octet-stream".to_string(),
            content_length: 0,
            computed_labels: Labels::new(),
        }
    }

    /// Creates a builder from an existing `RecordMeta` instance.
    pub fn builder_from(meta: RecordMeta) -> BuilderRecordMeta {
        BuilderRecordMeta {
            timestamp: meta.timestamp,
            state: meta.state,
            labels: meta.labels,
            content_type: meta.content_type,
            content_length: meta.content_length,
            computed_labels: meta.computed_labels,
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

    /// Returns a mutable reference to the labels associated with the record.
    pub fn labels_mut(&mut self) -> &mut Labels {
        &mut self.labels
    }

    /// For filtering unfinished records.
    pub fn state(&self) -> i32 {
        self.state
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

    /// Returns a mutable reference to the meta information about the record.
    fn meta_mut(&mut self) -> &mut RecordMeta;
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

    use rstest::{fixture, rstest};

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

    mod meta {
        use super::*;

        #[rstest]
        fn test_builder(meta: RecordMeta) {
            assert_eq!(meta.timestamp(), 1234567890);
            assert_eq!(meta.state(), 1);
            assert_eq!(meta.content_type(), "application/json");
            assert_eq!(meta.content_length(), 1024);
        }

        #[rstest]
        fn test_builder_from(meta: RecordMeta) {
            let builder = RecordMeta::builder_from(meta.clone());
            let new_meta = builder.build();

            assert_eq!(new_meta.timestamp(), 1234567890);
            assert_eq!(new_meta.state(), 1);
            assert_eq!(new_meta.content_type(), "application/json");
            assert_eq!(new_meta.content_length(), 1024);
        }

        #[rstest]
        fn test_meta_mut() {
            let mut record = MockRecord::new();
            let meta_mut = record.meta_mut();
            meta_mut
                .labels_mut()
                .insert("test_key".to_string(), "test_value".to_string());
            assert_eq!(
                record.meta().labels().get("test_key"),
                Some(&"test_value".to_string())
            );
        }

        #[fixture]
        fn meta() -> RecordMeta {
            RecordMeta::builder()
                .timestamp(1234567890)
                .state(1)
                .labels(Labels::new())
                .content_type("application/json".to_string())
                .content_length(1024)
                .computed_labels(Labels::new())
                .build()
        }
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

        fn meta_mut(&mut self) -> &mut RecordMeta {
            &mut self.metadata
        }
    }
}
