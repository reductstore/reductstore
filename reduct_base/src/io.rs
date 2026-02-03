// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod records;

use crate::error::ReductError;
use crate::Labels;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::{Read, Seek};
use std::time::Duration;

pub type WriteChunk = Result<Option<Bytes>, ReductError>;
pub type ReadChunk = Option<Result<Bytes, ReductError>>;

#[derive(Debug, Clone, PartialEq)]
pub struct RecordMeta {
    entry_name: String,
    timestamp: u64,
    state: i32,
    labels: Labels,
    content_type: String,
    content_length: u64,
    computed_labels: Labels,
}

pub struct BuilderRecordMeta {
    entry_name: String,
    timestamp: u64,
    state: i32,
    labels: Labels,
    content_type: String,
    content_length: u64,
    computed_labels: Labels,
}

impl BuilderRecordMeta {
    pub fn entry_name<T: ToString>(mut self, entry_name: T) -> Self {
        self.entry_name = entry_name.to_string();
        self
    }

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
            entry_name: self.entry_name,
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
            entry_name: String::new(),
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
            entry_name: meta.entry_name,
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

    /// Returns the entry name associated with the record, if any.
    pub fn entry_name(&self) -> &str {
        &self.entry_name
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

pub type BoxedReadRecord = Box<dyn ReadRecord + Send + Sync>;

/// Represents a record in the storage engine that can be read as a stream of bytes.
pub trait ReadRecord: Read + Seek {
    /// Reads a chunk of the record content.
    fn read_chunk(&mut self) -> ReadChunk;

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
}
