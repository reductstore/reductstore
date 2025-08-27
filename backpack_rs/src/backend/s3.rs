// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::StorageBackend;
use aws_config::{Region, SdkConfig};
use aws_sdk_s3::{Client, Config};
use std::path::PathBuf;

pub(crate) struct S3BackendSettings {
    pub local_cache_path: PathBuf,
    pub region: String,
}

pub(crate) struct S3Backend {
    settings: S3BackendSettings,
    wrapper: S3ClientWrapper,
}

struct S3ClientWrapper {
    client: Client,
    config: Config,
    rt: tokio::runtime::Runtime,
}

impl S3ClientWrapper {
    pub fn new(config: Config) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let client = Client::from_conf(config.clone());
        S3ClientWrapper { client, config, rt }
    }

    pub fn download_object(
        &self,
        bucket: &str,
        key: &str,
        dest: &PathBuf,
    ) -> Result<(), aws_sdk_s3::Error> {
        // let client = self.client.clone();
        // let bucket = bucket.to_string();
        // let key = key.to_string();
        // let dest = dest.clone();
        //
        // self.rt.block_on(async move {
        //     let resp = client.get_object().bucket(bucket).key(key).send().await?;
        //
        //     let data = resp.body.collect().await?;
        //     std::fs::write(dest, data.into_bytes())?;
        //     Ok(())
        // })

        todo!()
    }
}

impl S3Backend {
    pub fn new(settings: S3BackendSettings) -> Self {
        let conf = Config::builder()
            .region(Region::new(settings.region.clone()))
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(conf.clone());

        S3Backend {
            settings,
            wrapper: S3ClientWrapper::new(conf),
        }
    }
}

impl StorageBackend for S3Backend {
    fn path(&self) -> &std::path::PathBuf {
        todo!()
    }

    fn rename(&self, _from: &std::path::Path, _to: &std::path::Path) -> std::io::Result<()> {
        todo!()
    }

    fn remove(&self, _path: &std::path::Path) -> std::io::Result<()> {
        todo!()
    }

    fn remove_dir_all(&self, _path: &std::path::Path) -> std::io::Result<()> {
        todo!()
    }

    fn create_dir_all(&self, _path: &std::path::Path) -> std::io::Result<()> {
        todo!()
    }

    fn read_dir(&self, _path: &std::path::Path) -> std::io::Result<std::fs::ReadDir> {
        todo!()
    }

    fn try_exists(&self, path: &std::path::Path) -> std::io::Result<bool> {
        // check cache first and then load from s3 if not in cache
        let cache_path = self.settings.local_cache_path.join(path);
        if cache_path.exists() {
            return Ok(true);
        }

        todo!()

        // check in s3
        // let bucket = "your-bucket-name"; // replace with your bucket name
        // let key = path.to_str().unwrap(); // convert path to string
        // let resp = self.s3_client.head_object().bucket(bucket).key(key).send();
    }
}
