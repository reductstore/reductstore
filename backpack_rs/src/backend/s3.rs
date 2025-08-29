// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::StorageBackend;
use crate::error::Error;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{Client, Config};
use log::{debug, info};
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

struct S3ClientWrapper {
    bucket: String,
    client: Arc<Client>,
    rt: Arc<Runtime>,
}

impl S3ClientWrapper {
    pub fn new(settings: S3BackendSettings) -> Self {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("s3-client-worker")
                .enable_all()
                .build()
                .unwrap(),
        );

        info!("Clearing S3 cache path: {:?}", settings.cache_path);
        if settings.cache_path.exists() {
            fs::remove_dir_all(&settings.cache_path).unwrap_or_else(|e| {
                panic!(
                    "Failed to clear S3 cache path {:?}: {}",
                    settings.cache_path, e
                )
            });
        }

        info!("Initializing S3 client for bucket: {}", settings.bucket);
        let base = block_in_place(|| {
            rt.block_on(
                aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(settings.region.clone()))
                    .load(),
            )
        });

        let creds = Credentials::from_keys(
            settings.access_key.clone(),
            settings.secret_key.clone(),
            None,
        );
        let conf = aws_sdk_s3::config::Builder::from(&base)
            .endpoint_url(settings.endpoint)
            .credentials_provider(creds)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(conf.clone());

        S3ClientWrapper {
            client: Arc::new(client),
            bucket: settings.bucket,
            rt,
        }
    }

    pub fn download_object(&self, key: &str, dest: &PathBuf) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        block_in_place(move || {
            rt.block_on(async {
                let mut resp = client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 get_object error: {}", e.message().unwrap_or("")),
                        )
                    })?;
                let mut file = tokio::fs::File::create(dest).await?;

                while let Some(chunk) = resp.body.next().await {
                    let data = chunk?;
                    file.write_all(&data).await?;
                }
                Ok(())
            })
        })
    }

    fn upload_object(&self, key: &str, src: &PathBuf) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let data = std::fs::read(src)?;
        let rt = Arc::clone(&self.rt);
        block_in_place(move || {
            rt.block_on(async {
                client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(data.into())
                    .set_checksum_algorithm(None)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 put_object error: {}", e.message().unwrap_or("")),
                        )
                    })?;
                Ok(())
            })
        })
    }

    pub(crate) fn create_dir_all(&self, key: &str) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);

        let dir_key = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{}/", key)
        };

        block_in_place(|| {
            rt.block_on(async {
                client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(dir_key)
                    .body(Vec::new().into())
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("S3 put_object error: {}", e))
                    })?;
                Ok(())
            })
        })
    }

    pub(crate) fn list_objects(&self, key: &str) -> Result<Vec<String>, io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        block_in_place(|| {
            rt.block_on(async {
                let mut keys = Vec::new();
                let mut continuation_token = None;

                loop {
                    let resp = client
                        .list_objects_v2()
                        .bucket(&self.bucket)
                        .set_continuation_token(continuation_token.clone())
                        .prefix(key)
                        .send()
                        .await
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("S3 list_objects_v2 error: {}", e.message().unwrap()),
                            )
                        })?;

                    for object in resp.contents() {
                        if let Some(key) = object.key() {
                            keys.push(key.to_string());
                        }
                    }

                    if resp.is_truncated().unwrap_or(false) {
                        continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                    } else {
                        break;
                    }
                }

                Ok(keys)
            })
        })
    }

    fn remove_object(&self, key: &str) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        block_in_place(|| {
            rt.block_on(async {
                client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 delete_object error: {}", e.message().unwrap_or("")),
                        )
                    })?;
                Ok(())
            })
        })
    }
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

    fn remove_dir_all(&self, _path: &std::path::Path) -> std::io::Result<()> {
        todo!()
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

    fn read_dir(&self, path: &Path) -> io::Result<fs::ReadDir> {
        let s3_key = path
            .strip_prefix(&self.cache_path)
            .unwrap()
            .to_str()
            .unwrap_or("");
        for key in self.wrapper.list_objects(s3_key)? {
            if key.ends_with('/') {
                let local_dir = self.cache_path.join(&key);
                debug!(
                    "Creating local directory {} for S3 key: {}",
                    local_dir.to_str().unwrap_or(""),
                    key
                );
                fs::create_dir_all(local_dir)?;
            }
        }
        Ok(fs::read_dir(path)?)
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
            "Checking and downloading S3 key: {} to local path: {}",
            s3_key,
            full_path.display()
        );
        match self.wrapper.download_object(s3_key, &full_path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
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
}
