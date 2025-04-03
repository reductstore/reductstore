// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::ReductError;
use crate::io::ReadRecord;
use crate::msg::entry_api::QueryEntry;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Clone)]
pub struct IoExtensionInfo {
    name: String,
    version: String,
}

pub struct IoExtensionInfoBuilder {
    name: String,
    version: String,
}

impl IoExtensionInfoBuilder {
    fn new() -> Self {
        Self {
            name: String::new(),
            version: String::new(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn version(mut self, version: &str) -> Self {
        self.version = version.to_string();
        self
    }

    pub fn build(self) -> IoExtensionInfo {
        IoExtensionInfo {
            name: self.name,
            version: self.version,
        }
    }
}

impl IoExtensionInfo {
    pub fn builder() -> IoExtensionInfoBuilder {
        IoExtensionInfoBuilder::new()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

pub type BoxedReadRecord = Box<dyn ReadRecord + Send + Sync>;

/// The status of the processing of a record.
///
/// The three possible states allow to aggregate records on the extension side.
pub enum ProcessStatus {
    Ready(Result<BoxedReadRecord, ReductError>),
    NotReady,
    Stop,
}

impl Debug for ProcessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessStatus::Ready(_) => write!(f, "Ready(?)"),
            ProcessStatus::NotReady => write!(f, "NotReady"),
            ProcessStatus::Stop => write!(f, "Stop"),
        }
    }
}

pub trait IoExtension {
    fn info(&self) -> &IoExtensionInfo;

    fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query: &QueryEntry,
    ) -> Result<(), ReductError>;

    fn next_processed_record(&self, query_id: u64, record: BoxedReadRecord) -> ProcessStatus;
}
