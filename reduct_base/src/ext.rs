// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::ReductError;
use crate::msg::entry_api::QueryEntry;
use crate::Labels;
use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug)]
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

    pub fn name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub fn version(mut self, version: String) -> Self {
        self.version = version;
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

pub trait IoExtension {
    fn info(&self) -> IoExtensionInfo;

    fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query: &QueryEntry,
    ) -> Result<(), ReductError>;
}
