// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::s3::S3BackendSettings;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError::NotFound;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use log::info;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::task::block_in_place;
pub(super) struct S3ClientWrapper {
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
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
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
        let key = format!("r/{}", key);

        block_in_place(move || {
            rt.block_on(async {
                let mut resp = client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 get_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("")
                            ),
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

    pub(crate) fn upload_object(&self, key: &str, src: &PathBuf) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let key = format!("r/{}", key);

        block_in_place(move || {
            rt.block_on(async {
                let stream = ByteStream::from_path(src).await?;

                client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .body(stream)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 put_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("")
                            ),
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
            format!("r/{}", key)
        } else {
            format!("r/{}/", key)
        };

        block_in_place(|| {
            rt.block_on(async {
                client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&dir_key)
                    .body(Vec::new().into())
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 put_object error bucket={}, key={}: {}",
                                &self.bucket,
                                dir_key,
                                e.message().unwrap_or("")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }

    pub(crate) fn list_objects(
        &self,
        key: &str,
        recursive: bool,
    ) -> Result<Vec<String>, io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let prefix = if key.ends_with("/") || key.is_empty() {
            format!("r/{}", key)
        } else {
            format!("r/{}/", key)
        };

        block_in_place(|| {
            rt.block_on(async {
                let mut keys = HashSet::new();
                let mut continuation_token = None;

                loop {
                    let resp = client
                        .list_objects_v2()
                        .bucket(&self.bucket)
                        .set_continuation_token(continuation_token.clone())
                        .prefix(&prefix)
                        .send()
                        .await
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!(
                                    "S3 list_objects_v2 error bucket={}, key={}: {}",
                                    &self.bucket,
                                    &prefix,
                                    e.message().unwrap_or("")
                                ),
                            )
                        })?;

                    for object in resp.contents() {
                        // Didn't find a better way to filter out "subdirectories"
                        let Some(key) = object.key() else { continue };
                        if key == &prefix {
                            continue;
                        }

                        let key = key.strip_prefix(&prefix).unwrap_or(key);
                        if recursive {
                            keys.insert(key.to_string());
                        } else {
                            if let Some((first, _rest)) = key.split_once('/') {
                                // treat first segment as a "dir"
                                let dir = format!("{}/", first);
                                keys.insert(dir);
                            } else {
                                // no slash => top-level "file"
                                keys.insert(key.to_string());
                            }
                        }
                    }

                    if resp.is_truncated().unwrap_or(false) {
                        continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                    } else {
                        break;
                    }
                }

                let keys = keys.into_iter().collect::<Vec<_>>();
                Ok(keys)
            })
        })
    }

    pub(crate) fn remove_object(&self, key: &str) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let key = format!("r/{}", key);

        block_in_place(|| {
            rt.block_on(async {
                client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 delete_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }

    pub(crate) fn head_object(&self, key: &str) -> Result<bool, io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let key = format!("r/{}", key);

        block_in_place(|| {
            rt.block_on(async {
                let resp = client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send()
                    .await;

                match resp {
                    Ok(_) => Ok(true),
                    Err(e) => {
                        // Inspect the error
                        if let SdkError::ServiceError(err) = &e {
                            if err.err().is_not_found() {
                                return Ok(false); // Object does not exist
                            }
                        }
                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 head_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("")
                            ),
                        ))
                    }
                }
            })
        })
    }

    pub(crate) fn rename_object(&self, from: &str, to: &str) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let from_key = format!("r/{}", from);
        let to_key = format!("r/{}", to);

        info!(
            "Renaming S3 object from key: {} to key: {}",
            &from_key, &to_key
        );
        block_in_place(|| {
            rt.block_on(async {
                client
                    .rename_object()
                    .bucket(&self.bucket)
                    .rename_source(&from_key)
                    .key(&to_key)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 rename_object error bucket={}, from_key={}, to_key={}: {}",
                                &self.bucket,
                                &from_key,
                                &to_key,
                                e.message().unwrap_or("")
                            ),
                        )
                    })?;

                // Optionally, delete the source object after copying
                client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&from_key)
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 delete_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &from_key,
                                e.message().unwrap_or("")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }
}
