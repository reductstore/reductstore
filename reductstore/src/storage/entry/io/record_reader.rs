// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::RecordRx;
use crate::storage::proto::record::Label;
use crate::storage::proto::{ts_to_us, Record};
use bytes::Bytes;
use reduct_base::error::ReductError;
use tokio::sync::mpsc::Receiver;

pub struct RecordReader {
    rx: Option<RecordRx>,
    record: Record,
    last: bool,
}

impl RecordReader {
    pub fn new(rx: Receiver<Result<Bytes, ReductError>>, record: Record, last: bool) -> Self {
        RecordReader {
            rx: Some(rx),
            record,
            last,
        }
    }

    pub fn form_record(record: Record, last: bool) -> Self {
        RecordReader {
            rx: None,
            record,
            last,
        }
    }

    pub fn timestamp(&self) -> u64 {
        ts_to_us(self.record.timestamp.as_ref().unwrap())
    }

    pub fn content_type(&self) -> &str {
        self.record.content_type.as_str()
    }

    pub fn labels(&self) -> &Vec<Label> {
        &self.record.labels
    }

    pub fn content_length(&self) -> u64 {
        self.record.end - self.record.begin
    }

    pub fn only_metadata(&self) -> bool {
        self.rx.is_none()
    }

    /// Get the receiver to read the record content
    ///
    /// # Returns
    ///
    /// * `&mut Receiver<Result<Bytes, ReductError>>` - The receiver to read the record content
    ///
    /// # Panics
    ///
    /// Panics if the receiver isn't set (we read only metadata)
    pub fn rx(&mut self) -> &mut Receiver<Result<Bytes, ReductError>> {
        self.rx.as_mut().unwrap()
    }

    /// Consume the RecordReader and return the receiver to read the record content
    ///
    /// # Returns
    ///
    /// * `Receiver<Result<Bytes, ReductError>` - The receiver to read the record content
    ///
    /// # Panics
    ///
    /// Panics if the receiver isn't set (we read only metadata)
    pub fn into_rx(self) -> Receiver<Result<Bytes, ReductError>> {
        self.rx.unwrap()
    }

    pub fn last(&self) -> bool {
        self.last
    }

    pub fn record(&self) -> &Record {
        &self.record
    }
}
