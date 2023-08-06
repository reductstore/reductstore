// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod query;
pub mod read_record;
pub mod write_record;

use bytes::{Bytes, BytesMut};

use futures::stream::Stream;

use futures_util::StreamExt;
use reduct_base::error::HttpError;

use std::collections::HashMap;
use std::pin::Pin;

use std::time::SystemTime;

pub type Labels = HashMap<String, String>;
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Bytes, HttpError>>>>;

pub use write_record::WriteRecordBuilder;

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
    pub async fn bytes(mut self) -> Result<Bytes, HttpError> {
        if let Some(data) = &mut self.data {
            let mut bytes = BytesMut::new();
            while let Some(chunk) = data.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            return Ok(bytes.into());
        } else {
            panic!("Record has no data");
        }
    }

    pub fn stream_bytes(self) -> Pin<Box<dyn Stream<Item = Result<Bytes, HttpError>>>> {
        if let Some(data) = self.data {
            return data;
        } else {
            panic!("Record has no data");
        }
    }
}

pub(crate) fn from_system_time(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
