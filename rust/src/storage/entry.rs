// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;

/// Entry is a time series in a bucket.
#[derive(PartialEq, Debug)]
pub struct Entry {
    name: String,
    path: PathBuf,
    options: EntryOptions,
    block_index: BTreeSet<u64>,
}

/// EntryOptions is the options for creating a new entry.
#[derive(PartialEq, Debug)]
pub struct EntryOptions {
    pub max_block_size: u64,
    pub max_block_records: u64,
}

impl Entry {
    pub fn new(name: &str, path: PathBuf, options: EntryOptions) -> Self {
        Self {
            name: name.to_string(),
            path,
            options,
            block_index: BTreeSet::new(),
        }
    }

    pub fn restore(path: PathBuf, options: EntryOptions) -> Result<Self, HTTPError> {
        Ok(Self {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            path,
            options,
            block_index: BTreeSet::new(),
        })
    }

    pub fn begin_write(&self, time: u64, content_size: usize) -> Result<Box<dyn Write>, HTTPError> {
        enum RecordType {
            Latest,
            Belated,
            BelatedFirst,
        }

        Err(HTTPError::internal_server_error("Not implemented"))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
