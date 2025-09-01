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
use aws_sdk_s3::Client;
use log::info;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};
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

    pub(crate) fn upload_object(&self, key: &str, src: &PathBuf) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let data = std::fs::read(src)?;
        let rt = Arc::clone(&self.rt);
        let key = format!("r/{}", key);

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
            format!("r/{}", key)
        } else {
            format!("r/{}/", key)
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
                                format!("S3 list_objects_v2 error: {}", e.message().unwrap()),
                            )
                        })?;

                    for object in resp.contents() {
                        // Didn't find a better way to filter out "subdirectories"
                        let Some(key) = object.key() else { continue };
                        if key == &prefix {
                            continue;
                        }

                        let key = key.strip_prefix(&prefix).unwrap_or(key);
                        if let Some((first, rest)) = key.split_once('/') {
                            // treat first segment as a "dir"
                            let dir = format!("{}/", first);
                            keys.insert(dir);
                        } else {
                            // no slash => top-level "file"
                            keys.insert(key.to_string());
                        }
                    }

                    if resp.is_truncated().unwrap_or(false) {
                        continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                    } else {
                        break;
                    }
                }

                Ok(keys.into_iter().collect())
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

    pub(crate) fn head_object(&self, key: &str) -> Result<bool, io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let key = format!("r/{}", key);

        block_in_place(|| {
            rt.block_on(async {
                let resp = client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(key)
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
                            format!("S3 head_object error: {}", e.message().unwrap_or("")),
                        ))
                    }
                }
            })
        })
    }
}
