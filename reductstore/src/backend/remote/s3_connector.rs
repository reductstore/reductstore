// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::remote::RemoteBackendSettings;
use crate::backend::remote::RemoteStorageConnector;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use log::{debug, error, info};
use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::task::block_in_place;
pub(super) struct S3Connector {
    bucket: String,
    client: Arc<Client>,
    rt: Arc<Runtime>,
}

impl S3Connector {
    pub fn new(settings: RemoteBackendSettings) -> Self {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("remote-client-worker")
                .enable_all()
                .build()
                .unwrap(),
        );

        let mut base_config = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = settings.region.as_ref() {
            base_config = base_config.region(Region::new(region.clone()));
        }

        info!("Initializing S3 client for bucket: {}", settings.bucket);
        let base = block_in_place(|| rt.block_on(base_config.load()));

        let creds = Credentials::from_keys(
            settings.access_key.clone(),
            settings.secret_key.clone(),
            None,
        );
        let conf = aws_sdk_s3::config::Builder::from(&base)
            .set_endpoint_url(settings.endpoint)
            .clone()
            .credentials_provider(creds)
            .force_path_style(true)
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .build();

        let client = Client::from_conf(conf.clone());

        S3Connector {
            client: Arc::new(client),
            bucket: settings.bucket,
            rt,
        }
    }
}

impl RemoteStorageConnector for S3Connector {
    fn download_object(&self, key: &str, dest: &PathBuf) -> Result<(), io::Error> {
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
                        error!("S3 get_object error: {}", DisplayErrorContext(&e));
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 get_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("connection error")
                            ),
                        )
                    })?;
                let mut file = tokio::fs::File::create(dest).await?;

                while let Some(chunk) = resp.body.next().await {
                    let data = chunk?;
                    file.write_all(&data).await?;
                }
                file.flush().await?;
                file.sync_all().await?;
                Ok(())
            })
        })
    }
    fn upload_object(&self, key: &str, src: &PathBuf) -> Result<(), io::Error> {
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
                        error!("S3 put_object error: {}", DisplayErrorContext(&e));
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 put_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("connection error")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }
    fn create_dir_all(&self, key: &str) -> Result<(), io::Error> {
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
                        error!("S3 put_object: {}", DisplayErrorContext(&e));
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 put_object error bucket={}, key={}: {}",
                                &self.bucket,
                                dir_key,
                                e.message().unwrap_or("connection error")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }
    fn list_objects(&self, key: &str, recursive: bool) -> Result<Vec<String>, io::Error> {
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
                            error!("S3 list_objects_v2 error: {}", DisplayErrorContext(&e));
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!(
                                    "S3 list_objects_v2 error bucket={}, key={}: {}",
                                    &self.bucket,
                                    &prefix,
                                    e.message().unwrap_or("connection error")
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
    fn remove_object(&self, key: &str) -> Result<(), io::Error> {
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
                        error!("S3 delete_object error: {}", DisplayErrorContext(&e));
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 delete_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("connection error")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }
    fn head_object(&self, key: &str) -> Result<bool, io::Error> {
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
                        error!("S3 head_object error: {}", DisplayErrorContext(&e));

                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 head_object error bucket={}, key={}: {}",
                                &self.bucket,
                                &key,
                                e.message().unwrap_or("connection error")
                            ),
                        ))
                    }
                }
            })
        })
    }
    fn rename_object(&self, from: &str, to: &str) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let from_key = format!("r/{}", from);
        let to_key = format!("r/{}", to);

        debug!(
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
                                e.message().unwrap_or("connection error")
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
                                e.message().unwrap_or("connection error")
                            ),
                        )
                    })?;
                Ok(())
            })
        })
    }
}

// Dummy tests - we can't do real S3 operations in CI
// These tests are just to ensure that the code paths are correct
// and that error handling works as expected.
#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::fs;
    use tempfile::tempdir;

    #[rstest]
    fn download_object(connector: S3Connector) {
        let key = "test_download.txt";
        let dest = PathBuf::from("/tmp/test_download.txt");

        assert_eq!(
            connector
                .download_object(key, &dest)
                .err()
                .unwrap()
                .to_string(),
            "S3 get_object error bucket=test-bucket, key=r/test_download.txt: connection error"
        );
    }

    #[rstest]
    fn upload_object(connector: S3Connector, path: PathBuf) {
        let key = "test_upload.txt";
        let src = path.join("test_upload.txt");
        fs::write(&src, b"test upload content").unwrap();

        assert_eq!(
            connector
                .upload_object(key, &src)
                .err()
                .unwrap()
                .to_string(),
            "S3 put_object error bucket=test-bucket, key=r/test_upload.txt: connection error"
        );
    }

    #[rstest]
    fn create_dir_all(connector: S3Connector) {
        let key = "test_dir/";

        assert_eq!(
            connector.create_dir_all(key).err().unwrap().to_string(),
            "S3 put_object error bucket=test-bucket, key=r/test_dir/: connection error"
        );
    }

    #[rstest]
    fn list_objects(connector: S3Connector) {
        let key = "test_list/";
        let recursive = false;

        assert_eq!(
            connector
                .list_objects(key, recursive)
                .err()
                .unwrap()
                .to_string(),
            "S3 list_objects_v2 error bucket=test-bucket, key=r/test_list/: connection error"
        );
    }

    #[rstest]
    fn remove_object(connector: S3Connector) {
        let key = "test_remove.txt";

        assert_eq!(
            connector.remove_object(key).err().unwrap().to_string(),
            "S3 delete_object error bucket=test-bucket, key=r/test_remove.txt: connection error"
        );
    }

    #[rstest]
    fn head_object(connector: S3Connector) {
        let key = "test_head.txt";

        assert_eq!(
            connector.head_object(key).err().unwrap().to_string(),
            "S3 head_object error bucket=test-bucket, key=r/test_head.txt: connection error"
        );
    }

    #[rstest]
    fn rename_object(connector: S3Connector) {
        let from = "test_rename_from.txt";
        let to = "test_rename_to.txt";

        assert_eq!(connector.rename_object(from, to).err().unwrap().to_string(),
            "S3 rename_object error bucket=test-bucket, from_key=r/test_rename_from.txt, to_key=r/test_rename_to.txt: connection error"
        );
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    fn connector(settings: RemoteBackendSettings) -> S3Connector {
        S3Connector::new(settings)
    }

    #[fixture]
    fn settings() -> RemoteBackendSettings {
        RemoteBackendSettings {
            connector_type: Default::default(),
            cache_path: Default::default(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: "http://xxxxx:9000".to_string(), // we do just a dry run
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            cache_size: 0,
        }
    }
}
