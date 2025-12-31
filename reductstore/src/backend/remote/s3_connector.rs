// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::remote::RemoteBackendSettings;
use crate::backend::remote::RemoteStorageConnector;
use crate::backend::ObjectMetadata;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::StorageClass;
use aws_sdk_s3::Client;
use log::{debug, error, info};
use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::task::block_in_place;
use tokio::time::timeout;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct S3Connector {
    bucket: String,
    client: Arc<Client>,
    rt: Arc<Runtime>,
    prefix: &'static str,
    default_storage_class: Option<StorageClass>,
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

        let base_config = aws_config::defaults(BehaviorVersion::latest()).region(
            settings
                .region
                .as_ref()
                .map(|r| Region::new(r.clone()))
                .unwrap_or_else(|| Region::new("notset".to_string())),
        );

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

        let default_storage_class =
            match settings.default_storage_class.as_ref().map(|s| s.as_str()) {
                Some("STANDARD") => Some(StorageClass::Standard),
                Some("STANDARD_IA") => Some(StorageClass::StandardIa),
                Some("INTELLIGENT_TIERING") => Some(StorageClass::IntelligentTiering),
                Some("ONEZONE_IA") => Some(StorageClass::OnezoneIa),
                Some("EXPRESS_ONEZONE") => Some(StorageClass::ExpressOnezone),
                Some("GLACIER_IR") => Some(StorageClass::GlacierIr),
                Some("GLACIER") => Some(StorageClass::Glacier),
                Some("DEEP_ARCHIVE") => Some(StorageClass::DeepArchive),
                Some("OUTPOSTS") => Some(StorageClass::Outposts),
                Some("REDUCED_REDUNDANCY") => Some(StorageClass::ReducedRedundancy),
                Some(other) => {
                    error!("Unknown storage class: {}, defaulting to None", other);
                    None
                }
                _ => None,
            };

        S3Connector {
            client: Arc::new(client),
            bucket: settings.bucket,
            rt,
            prefix: "r/",
            default_storage_class,
        }
    }

    fn timeout_error(&self, op: &str, key: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "S3 {op} timeout bucket={}, key={}: exceeded {:?}",
                self.bucket, key, REQUEST_TIMEOUT
            ),
        )
    }
}

impl RemoteStorageConnector for S3Connector {
    fn download_object(&self, key: &str, dest: &PathBuf) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let key = format!("{}{}", self.prefix, key);
        let bucket = self.bucket.clone();
        let key_for_log = key.clone();

        block_in_place(move || {
            rt.block_on(async {
                let mut resp = match timeout(
                    REQUEST_TIMEOUT,
                    client.get_object().bucket(&bucket).key(&key).send(),
                )
                .await
                {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(e)) => {
                        error!("S3 get_object error: {}", DisplayErrorContext(&e));
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 get_object error bucket={}, key={}: {}",
                                &bucket,
                                &key_for_log,
                                e.message().unwrap_or("connection error")
                            ),
                        ));
                    }
                    Err(_) => return Err(self.timeout_error("get_object", &key_for_log)),
                };

                let mut file = tokio::fs::File::create(dest).await?;

                while let Some(chunk) = match timeout(REQUEST_TIMEOUT, resp.body.next()).await {
                    Ok(next) => next,
                    Err(_) => return Err(self.timeout_error("get_object stream", &key_for_log)),
                } {
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
        let key = format!("{}{}", self.prefix, key);
        let bucket = self.bucket.clone();
        let key_for_log = key.clone();

        let storage_class = if key.ends_with(".blk") {
            self.default_storage_class.clone()
        } else {
            None
        };

        block_in_place(move || {
            rt.block_on(async {
                let stream = ByteStream::from_path(src).await?;

                match timeout(
                    REQUEST_TIMEOUT,
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .set_storage_class(storage_class)
                        .body(stream)
                        .send(),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("S3 put_object error: {}", DisplayErrorContext(&e));
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 put_object error bucket={}, key={}: {}",
                                &bucket,
                                &key_for_log,
                                e.message().unwrap_or("connection error")
                            ),
                        ));
                    }
                    Err(_) => return Err(self.timeout_error("put_object", &key_for_log)),
                }
                Ok(())
            })
        })
    }
    fn create_dir_all(&self, key: &str) -> Result<(), io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let bucket = self.bucket.clone();

        let dir_key = if key.ends_with('/') {
            format!("{}{}", self.prefix, key)
        } else {
            format!("{}{}/", self.prefix, key)
        };
        let dir_key_log = dir_key.clone();

        block_in_place(|| {
            rt.block_on(async {
                match timeout(
                    REQUEST_TIMEOUT,
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&dir_key)
                        .body(Vec::new().into())
                        .send(),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("S3 put_object: {}", DisplayErrorContext(&e));
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 put_object error bucket={}, key={}: {}",
                                &bucket,
                                dir_key_log,
                                e.message().unwrap_or("connection error")
                            ),
                        ));
                    }
                    Err(_) => return Err(self.timeout_error("put_object", &dir_key_log)),
                }
                Ok(())
            })
        })
    }
    fn list_objects(&self, key: &str, recursive: bool) -> Result<Vec<String>, io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let prefix = if key.ends_with("/") || key.is_empty() {
            format!("{}{}", self.prefix, key)
        } else {
            format!("{}{}/", self.prefix, key)
        };
        let bucket = self.bucket.clone();
        let prefix_log = prefix.clone();

        block_in_place(|| {
            rt.block_on(async {
                let mut keys = HashSet::new();
                let mut continuation_token = None;

                loop {
                    let resp = match timeout(
                        REQUEST_TIMEOUT,
                        client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .set_continuation_token(continuation_token.clone())
                            .prefix(&prefix)
                            .send(),
                    )
                    .await
                    {
                        Ok(Ok(resp)) => resp,
                        Ok(Err(e)) => {
                            error!("S3 list_objects_v2 error: {}", DisplayErrorContext(&e));
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!(
                                    "S3 list_objects_v2 error bucket={}, key={}: {}",
                                    &bucket,
                                    &prefix_log,
                                    e.message().unwrap_or("connection error")
                                ),
                            ));
                        }
                        Err(_) => return Err(self.timeout_error("list_objects_v2", &prefix_log)),
                    };

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
        let key = format!("{}{}", self.prefix, key);
        let bucket = self.bucket.clone();
        let key_for_log = key.clone();

        block_in_place(|| {
            rt.block_on(async {
                match timeout(
                    REQUEST_TIMEOUT,
                    client.delete_object().bucket(&bucket).key(&key).send(),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("S3 delete_object error: {}", DisplayErrorContext(&e));
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 delete_object error bucket={}, key={}: {}",
                                &bucket,
                                &key_for_log,
                                e.message().unwrap_or("connection error")
                            ),
                        ));
                    }
                    Err(_) => return Err(self.timeout_error("delete_object", &key_for_log)),
                }
                Ok(())
            })
        })
    }
    fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>, io::Error> {
        let client = Arc::clone(&self.client);
        let rt = Arc::clone(&self.rt);
        let key = format!("{}{}", self.prefix, key);
        let bucket = self.bucket.clone();
        let key_for_log = key.clone();
        block_in_place(|| {
            rt.block_on(async {
                let resp = timeout(
                    REQUEST_TIMEOUT,
                    client.head_object().bucket(&bucket).key(&key).send(),
                )
                .await;

                match resp.map_err(|_| self.timeout_error("head_object", &key_for_log))? {
                    Ok(output) => {
                        let metadata = ObjectMetadata {
                            size: output.content_length(),
                            modified_time: output.last_modified().map(|dt| {
                                SystemTime::UNIX_EPOCH
                                    + std::time::Duration::from_secs(dt.secs() as u64)
                            }),
                        };
                        Ok(Some(metadata))
                    }
                    Err(e) => {
                        // Inspect the error
                        if let SdkError::ServiceError(err) = &e {
                            if err.err().is_not_found() {
                                return Ok(None); // Object does not exist
                            }
                        }
                        error!("S3 head_object error: {}", DisplayErrorContext(&e));

                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 head_object error bucket={}, key={}: {}",
                                &bucket,
                                &key_for_log,
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
        let from_key = format!("{}{}", self.prefix, from);
        let to_key = format!("{}{}", self.prefix, to);
        let bucket = self.bucket.clone();
        let from_key_log = from_key.clone();
        let to_key_log = to_key.clone();

        debug!(
            "Renaming S3 object from key: {} to key: {}",
            &from_key, &to_key
        );
        block_in_place(|| {
            rt.block_on(async {
                match timeout(
                    REQUEST_TIMEOUT,
                    client
                        .rename_object()
                        .bucket(&bucket)
                        .rename_source(&from_key)
                        .key(&to_key)
                        .send(),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 rename_object error bucket={}, from_key={}, to_key={}: {}",
                                &bucket,
                                &from_key_log,
                                &to_key_log,
                                e.message().unwrap_or("connection error")
                            ),
                        ));
                    }
                    Err(_) => return Err(self.timeout_error("rename_object", &from_key_log)),
                }

                // Optionally, delete the source object after copying
                match timeout(
                    REQUEST_TIMEOUT,
                    client.delete_object().bucket(&bucket).key(&from_key).send(),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 delete_object error bucket={}, key={}: {}",
                                &bucket,
                                &from_key_log,
                                e.message().unwrap_or("connection error")
                            ),
                        ));
                    }
                    Err(_) => return Err(self.timeout_error("delete_object", &from_key_log)),
                }
                Ok(())
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::fs;
    use tempfile::tempdir;

    // Dummy tests without S3 connection
    // These tests are just to ensure that the code paths are correct
    // and that error handling works as expected.
    mod dummy {
        use super::*;

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

        #[rstest]
        #[case("STANDARD", Some(StorageClass::Standard))]
        #[case("STANDARD_IA", Some(StorageClass::StandardIa))]
        #[case("INTELLIGENT_TIERING", Some(StorageClass::IntelligentTiering))]
        #[case("ONEZONE_IA", Some(StorageClass::OnezoneIa))]
        #[case("EXPRESS_ONEZONE", Some(StorageClass::ExpressOnezone))]
        #[case("GLACIER_IR", Some(StorageClass::GlacierIr))]
        #[case("GLACIER", Some(StorageClass::Glacier))]
        #[case("DEEP_ARCHIVE", Some(StorageClass::DeepArchive))]
        #[case("OUTPOSTS", Some(StorageClass::Outposts))]
        #[case("REDUCED_REDUNDANCY", Some(StorageClass::ReducedRedundancy))]
        #[case("UNKNOWN_CLASS", None)]
        fn test_storage_class_mapping(
            #[case] input: &str,
            #[case] expected: Option<StorageClass>,
            settings: RemoteBackendSettings,
        ) {
            let mut custom_settings = settings;
            custom_settings.default_storage_class = Some(input.to_string());
            let connector = S3Connector::new(custom_settings);
            assert_eq!(connector.default_storage_class, expected);
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
                backend_type: Default::default(),
                cache_path: Default::default(),
                bucket: "test-bucket".to_string(),
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://xxxxx:9000".to_string()), // we do just a dry run
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                cache_size: 0,
                default_storage_class: None,
            }
        }
    }

    #[cfg(feature = "ci")]
    mod ci {
        use super::*;
        use crate::backend::BackendType;
        use crate::core::env;
        use crate::core::env::StdEnvGetter;
        use serial_test::serial;
        use tempfile::tempdir;

        #[rstest]
        #[serial]
        fn download_object(connector: S3Connector, path: PathBuf) {
            let key = "test/test.txt";
            let dest = path.join("downloaded_test.txt");
            assert!(!dest.exists());

            (connector.download_object(key, &dest).unwrap());
            assert!(dest.exists());
            let content = std::fs::read_to_string(&dest).unwrap();
            assert_eq!(content, "This is a test file for download.\n");
        }

        #[rstest]
        #[serial]
        fn upload_object(connector: S3Connector, path: PathBuf) {
            let key = "test/uploaded_test.txt";
            let src = path.join("uploaded_test.txt");
            fs::write(&src, b"This is a test file for upload.\n").unwrap();

            (connector.upload_object(key, &src).unwrap());
            assert!(connector.head_object(key).unwrap().is_some());
        }

        #[rstest]
        #[serial]
        fn upload_object_with_storage_class(connector: S3Connector, path: PathBuf) {
            let key = "test/uploaded_test.blk";
            let src = path.join("uploaded_test.blk");
            fs::write(
                &src,
                b"This is a test file for upload with storage class.\n",
            )
            .unwrap();

            (connector.upload_object(key, &src).unwrap());
            assert!(connector.head_object(key).unwrap().is_some());
        }

        #[rstest]
        #[serial]
        fn create_dir_all(connector: S3Connector) {
            let key = "test/new_dir/";

            (connector.create_dir_all(key).unwrap());
            assert!(connector.head_object(key).unwrap().is_some());
        }

        #[rstest]
        #[serial]
        fn list_objects_recursive(connector: S3Connector) {
            connector.create_dir_all("test/subdir1/").unwrap();
            connector.create_dir_all("test/subdir1/subdir2").unwrap();

            let objects = connector.list_objects("", true).unwrap();
            assert_eq!(objects.len(), 3);
            assert!(objects.contains(&"test/test.txt".to_string()));
            assert!(objects.contains(&"test/subdir1/".to_string()));
            assert!(objects.contains(&"test/subdir1/subdir2/".to_string()));
        }

        #[rstest]
        #[serial]
        fn list_objects_non_recursive(connector: S3Connector) {
            connector.create_dir_all("test/subdir1/").unwrap();
            connector.create_dir_all("test/subdir1/subdir2").unwrap();

            let objects = connector.list_objects("", false).unwrap();
            assert_eq!(objects.len(), 1);
            assert!(objects.contains(&"test/".to_string()));
        }

        #[rstest]
        #[serial]
        fn rename_object(connector: S3Connector) {
            let from = "test/uploaded_test.txt";
            let to = "test/renamed_test.txt";

            (connector.rename_object(from, to).unwrap());
            assert!(connector.head_object(from).unwrap().is_none());
            assert!(connector.head_object(to).unwrap().is_some());
        }

        #[rstest]
        #[serial]
        fn remove_object(connector: S3Connector) {
            let key = "test/uploaded_test.txt";

            (connector.remove_object(key).unwrap());
            assert!(connector.head_object(key).unwrap().is_none());
        }

        #[rstest]
        #[serial]
        fn head_object(connector: S3Connector) {
            let existing_key = "test/test.txt";
            let non_existing_key = "test/non_existing.txt";

            assert!(connector.head_object(existing_key).unwrap().is_some());
            assert!(connector.head_object(non_existing_key).unwrap().is_none());
        }

        #[fixture]
        fn path() -> PathBuf {
            tempdir().unwrap().keep()
        }

        #[fixture]
        fn connector(settings: RemoteBackendSettings) -> S3Connector {
            let mut connector = S3Connector::new(settings);
            connector.prefix = "ci/";

            for key in connector.list_objects("", true).unwrap() {
                connector
                    .remove_object(&key)
                    .expect("Failed to clean up S3 bucket");
            }

            let key = "test/test.txt";
            let src = tempdir().unwrap().keep().join("test.txt");
            fs::write(&src, b"This is a test file for download.\n").unwrap();
            connector
                .upload_object(key, &src)
                .expect("Failed to upload test file to S3");
            connector
        }

        #[fixture]
        fn settings() -> RemoteBackendSettings {
            let mut env = env::Env::new(StdEnvGetter::default());
            RemoteBackendSettings {
                backend_type: BackendType::S3,
                cache_path: tempdir().unwrap().keep(),
                bucket: env
                    .get_optional("MINIO_BUCKET")
                    .expect("MINIO_BUCKET must be set"),
                region: None,
                endpoint: Some(
                    env.get_optional("MINIO_ENDPOINT")
                        .unwrap_or("http://127.0.0.1:9000".to_string()),
                ),
                access_key: env
                    .get_optional("MINIO_ACCESS_KEY")
                    .unwrap_or("minioadmin".to_string()),
                secret_key: env
                    .get_optional("MINIO_SECRET_KEY")
                    .unwrap_or("minioadmin".to_string()),
                cache_size: 1000,
                default_storage_class: None,
            }
        }
    }
}
