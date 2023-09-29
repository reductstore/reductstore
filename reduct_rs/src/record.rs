// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod query;
pub mod read_record;
mod write_batched_records;
pub mod write_record;

use bytes::{Bytes, BytesMut};

use futures::stream::Stream;

use futures_util::StreamExt;
use reduct_base::error::ReductError;

use std::fmt::{Debug, Formatter};
use std::pin::Pin;

use std::time::SystemTime;

pub use reduct_base::batch::Labels;

pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Bytes, ReductError>>>>;

pub use write_record::WriteRecordBuilder;

pub trait RecordMut {
    /// Set the timestamp of the record to write as a unix timestamp in microseconds.
    fn set_timestamp_us(&mut self, timestamp: u64);

    /// Set the timestamp of the record to write.
    fn set_timestamp(&mut self, timestamp: SystemTime);

    /// Set the labels of the record to write.
    /// This replaces all existing labels.
    fn set_labels(&mut self, labels: Labels);

    /// Add a label to the record to write.
    fn add_label(&mut self, key: String, value: String);

    /// Set the content type of the record to write.
    fn set_content_type(&mut self, content_type: String);

    /// Set the content length of the record to write
    fn set_content_length(&mut self, content_length: usize);

    /// Set the content of the record
    fn set_bytes(&mut self, bytes: Bytes);

    /// Set the content of the record as a stream
    fn set_stream_bytes(&mut self, stream: RecordStream);
}

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
    pub fn bytes(mut self) -> Pin<Box<dyn futures::Future<Output = Result<Bytes, ReductError>>>> {
        Box::pin(async move {
            if let Some(mut data) = self.data {
                let mut bytes = BytesMut::new();
                while let Some(chunk) = data.next().await {
                    bytes.extend_from_slice(&chunk?);
                }
                Ok(bytes.into())
            } else {
                panic!("Record has no data");
            }
        })
    }

    /// Content of the record as a stream
    pub fn stream_bytes(self) -> Pin<Box<dyn Stream<Item = Result<Bytes, ReductError>>>> {
        if let Some(data) = self.data {
            data
        } else {
            panic!("Record has no data");
        }
    }
}

impl RecordMut for Record {
    /// Set the timestamp of the record to write as a unix timestamp in microseconds.
    fn set_timestamp_us(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    /// Set the timestamp of the record to write.
    fn set_timestamp(&mut self, timestamp: SystemTime) {
        self.timestamp = from_system_time(timestamp);
    }

    /// Set the labels of the record to write.
    /// This replaces all existing labels.
    fn set_labels(&mut self, labels: Labels) {
        self.labels = labels;
    }

    /// Add a label to the record to write.
    fn add_label(&mut self, key: String, value: String) {
        self.labels.insert(key, value);
    }

    /// Set the content type of the record to write.
    fn set_content_type(&mut self, content_type: String) {
        self.content_type = content_type;
    }

    /// Set the content length of the record to write
    fn set_content_length(&mut self, content_length: usize) {
        self.content_length = content_length;
    }

    /// Set the content of the record
    fn set_bytes(&mut self, bytes: Bytes) {
        self.data = Some(Box::pin(futures::stream::once(async move { Ok(bytes) })));
    }

    /// Set the content of the record as a stream
    fn set_stream_bytes(&mut self, stream: RecordStream) {
        self.data = Some(stream);
    }
}

pub(crate) fn from_system_time(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
