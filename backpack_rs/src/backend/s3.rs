// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod wrapper;

use crate::backend::s3::wrapper::S3ClientWrapper;
use crate::backend::StorageBackend;
use crate::error::Error;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::{Credentials, IntoShared};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::head_object::HeadObjectError::NotFound;
use aws_sdk_s3::{Client, Config};
use log::{debug, info};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;

pub(crate) struct S3BackendSettings {
    pub cache_path: PathBuf,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub bucket: String,
}

pub(crate) struct S3Backend {
    cache_path: PathBuf,
    wrapper: S3ClientWrapper,
}

impl S3Backend {
    pub fn new(settings: S3BackendSettings) -> Self {
        S3Backend {
            cache_path: settings.cache_path.clone(),
            wrapper: S3ClientWrapper::new(settings),
        }
    }
}

impl StorageBackend for S3Backend {
    fn path(&self) -> &std::path::PathBuf {
        &self.cache_path
    }

    fn rename(&self, _from: &std::path::Path, _to: &std::path::Path) -> std::io::Result<()> {
        todo!()
    }

    fn remove(&self, path: &std::path::Path) -> std::io::Result<()> {
        fs::remove_file(path)?;
        let s3_key = path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");

        debug!("Removing S3 object for key: {}", s3_key);
        self.wrapper.remove_object(s3_key)
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        fs::remove_dir_all(path)?;
        let s3_key = path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");

        debug!("Removing S3 directory for key: {}", s3_key);
        for key in self.wrapper.list_objects(&s3_key)? {
            self.wrapper
                .remove_object(format!("{}/{}", s3_key, key).as_str())?;
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let full_path = self.cache_path.join(path);
        fs::create_dir_all(&full_path)?;

        debug!("Creating S3 directory for key: {}", path.display());
        let key = full_path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");
        if key.is_empty() {
            return Ok(());
        }

        self.wrapper.create_dir_all(key)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let s3_key = path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");

        let mut paths = vec![];
        for key in self.wrapper.list_objects(s3_key)? {
            if key == s3_key {
                continue;
            }

            let local_path = self.cache_path.join(path).join(&key);
            if key.ends_with('/') {
                debug!(
                    "Creating local directory {} for S3 key: {}",
                    local_path.to_str().unwrap_or(""),
                    key
                );
                fs::create_dir_all(&local_path)?;
            }

            paths.push(local_path);
        }
        Ok(paths)
    }

    fn try_exists(&self, path: &Path) -> std::io::Result<bool> {
        // check cache first and then load from s3 if not in cache
        let full_path = self.cache_path.join(path);
        if full_path.exists() {
            return Ok(true);
        }
        let s3_key = full_path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");

        debug!(
            "Checking S3 key: {} to local path: {}",
            s3_key,
            full_path.display()
        );
        self.wrapper.head_object(s3_key)
    }

    fn sync(&self, full_path: &Path) -> std::io::Result<()> {
        // upload to s3
        let s3_key = full_path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");

        debug!(
            "Syncing local file {} to S3 key: {}",
            full_path.display(),
            s3_key
        );
        self.wrapper
            .upload_object(s3_key, &full_path.to_path_buf())?;

        Ok(())
    }

    fn download(&self, path: &std::path::Path) -> std::io::Result<()> {
        let full_path = self.cache_path.join(path);
        if full_path.exists() {
            return Ok(());
        }
        let s3_key = full_path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");

        debug!(
            "Downloading S3 key: {} to local path: {}",
            s3_key,
            full_path.display()
        );
        self.wrapper.download_object(s3_key, &full_path)
    }
}
