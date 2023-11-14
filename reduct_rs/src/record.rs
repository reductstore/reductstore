// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod query;
pub mod read_record;
pub mod write_batched_records;
pub mod write_record;

use bytes::{Bytes, BytesMut};

use futures::stream::Stream;

use futures_util::StreamExt;
use reduct_base::error::ReductError;

use std::fmt::{Debug, Formatter};
use std::pin::Pin;

use async_stream::stream;

use std::time::SystemTime;

pub use reduct_base::Labels;

pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Bytes, ReductError>> + Send + Sync>>;

pub use write_record::WriteRecordBuilder;

impl Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("timestamp", &self.timestamp())
            .field("labels", &self.labels())
            .field("content_type", &self.content_type())
            .field("content_length", &self.content_length())
            .finish()
    }
}

/// A record is a timestamped piece of data with labels
pub struct Record {
    timestamp: u64,
    labels: Labels,
    content_type: String,
    content_length: usize,
    data: Option<RecordStream>,
}

pub struct RecordBuilder {
    record: Record,
}

impl Record {
    /// Unix timestamp in microseconds
    pub fn timestamp_us(&self) -> u64 {
        self.timestamp
    }

    /// Timestamp as a SystemTime
    pub fn timestamp(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + std::time::Duration::from_micros(self.timestamp)
    }

    /// Labels associated with the record
    pub fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Content type of the record
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Content length of the record
    pub fn content_length(&self) -> usize {
        self.content_length
    }

    /// Content of the record
    ///
    /// This consumes the record and returns bytes
    pub fn bytes(self) -> Pin<Box<dyn futures::Future<Output = Result<Bytes, ReductError>>>> {
        Box::pin(async move {
            if let Some(mut data) = self.data {
                let mut bytes = BytesMut::new();
                while let Some(chunk) = data.next().await {
                    bytes.extend_from_slice(&chunk?);
                }
                Ok(bytes.into())
            } else {
                Ok(Bytes::new())
            }
        })
    }

    /// Content of the record as a stream
    pub fn stream_bytes(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, ReductError>> + Sync + Send>> {
        if let Some(data) = self.data {
            data
        } else {
            let stream = stream! {
               yield Ok(Bytes::new());
            };
            Box::pin(stream)
        }
    }
}

impl RecordBuilder {
    pub fn new() -> Self {
        Self {
            record: Record {
                timestamp: from_system_time(SystemTime::now()),
                labels: Default::default(),
                content_type: "application/octet-stream".to_string(),
                content_length: 0,
                data: None,
            },
        }
    }

    /// Set the timestamp of the record to write as a unix timestamp in microseconds.
    pub fn timestamp_us(mut self, timestamp: u64) -> Self {
        self.record.timestamp = timestamp;
        self
    }

    /// Set the timestamp of the record to write.
    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.record.timestamp = from_system_time(timestamp);
        self
    }

    /// Set the labels of the record to write.
    /// This replaces all existing labels.
    pub fn labels(mut self, labels: Labels) -> Self {
        self.record.labels = labels;
        self
    }

    /// Add a label to the record to write.
    pub fn add_label(mut self, key: String, value: String) -> Self {
        self.record.labels.insert(key, value);
        self
    }

    /// Set the content type of the record to write.
    pub fn content_type(mut self, content_type: String) -> Self {
        self.record.content_type = content_type;
        self
    }

    /// Set the content length of the record to write
    pub fn content_length(mut self, content_length: usize) -> Self {
        self.record.content_length = content_length;
        self
    }

    /// Set the content of the record
    ///
    /// Overwrites content length
    pub fn data(mut self, bytes: Bytes) -> Self {
        self.record.content_length = bytes.len();
        self.record.data = Some(Box::pin(futures::stream::once(async move { Ok(bytes) })));
        self
    }

    /// Set the content of the record as a stream
    pub fn stream(mut self, stream: RecordStream) -> Self {
        self.record.data = Some(stream);
        self
    }

    /// Build the record
    /// This consumes the builder
    pub fn build(self) -> Record {
        self.record
    }
}

pub(crate) fn from_system_time(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
