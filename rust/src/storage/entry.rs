// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use std::path::PathBuf;

/// Entry is a time series in a bucket.
#[derive(PartialEq, Debug)]
pub struct Entry {
    name: String,
    path: PathBuf,
}

impl Entry {
    pub fn new(name: &str, path: PathBuf) -> Self {
        Self {
            name: name.to_string(),
            path,
        }
    }

    pub fn restore(path: PathBuf) -> Result<Self, HTTPError> {
        Ok(Self {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            path,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
