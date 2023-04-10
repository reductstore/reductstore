// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use std::path::PathBuf;
use crate::storage::proto::BucketInfo;

/// Bucket is a single storage bucket.
#[derive(PartialEq, Debug)]
pub struct Bucket {
    name: String,
    path: PathBuf,
}

impl Bucket {
    /// Create a new Bucket
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    /// * `path` - The path to folder with buckets
    pub(crate) fn new(name: &str, path: &PathBuf) -> Result<Bucket, HTTPError> {
        let path = path.join(name);
        std::fs::create_dir_all(&path)?;

        Ok(Bucket {
            name: name.to_string(),
            path,
        })
    }

    /// Restore a Bucket from disk
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the bucket
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub fn restore(path: PathBuf) -> Result<Bucket, HTTPError> {
        Ok(Bucket {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            path,
        })
    }

    /// Remove a Bucket from disk
    ///
    /// # Returns
    ///
    /// * `Result<(), HTTPError>` - The result or an HTTPError
    pub fn remove(&self) -> Result<(), HTTPError> {
        std::fs::remove_dir_all(&self.path)?;
        Ok(())
    }


    /// Return bucket stats
    pub fn info(&self) -> Result<BucketInfo, HTTPError> {
        Ok(BucketInfo {
            name: self.name.clone(),
            size: 0,
            entry_count: 0,
            oldest_record: 0,
            latest_record: 0,
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}
