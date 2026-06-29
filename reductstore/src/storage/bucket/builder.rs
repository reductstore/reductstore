// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::{normalize_entry_name, settings_for_entry, Bucket, MultiEntryQuery};
use crate::cfg::Cfg;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::bucket::settings::SETTINGS_NAME;
use crate::storage::entry::Entry;
use crate::storage::folder_keeper::FolderKeeper;
use crate::storage::in_flight::InFlightIoLimiter;
use crate::storage::proto::BucketSettings as ProtoBucketSettings;
use crate::storage::usage::UsageCounters;
use log::error;
use prost::bytes::Bytes;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::status::ResourceStatus;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct BucketBuilder {
    name: Option<String>,
    path: Option<PathBuf>,
    data_path: Option<PathBuf>,
    settings: Option<BucketSettings>,
    cfg: Option<Cfg>,
    io_limiter: Option<InFlightIoLimiter>,
    usage_counters: Option<Arc<UsageCounters>>,
}

impl BucketBuilder {
    pub(crate) fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub(crate) fn data_path(mut self, data_path: PathBuf) -> Self {
        self.data_path = Some(data_path);
        self
    }

    pub(crate) fn settings(mut self, settings: BucketSettings) -> Self {
        self.settings = Some(settings);
        self
    }

    pub(crate) fn cfg(mut self, cfg: Cfg) -> Self {
        self.cfg = Some(cfg);
        self
    }

    pub(crate) fn io_limiter(mut self, io_limiter: InFlightIoLimiter) -> Self {
        self.io_limiter = Some(io_limiter);
        self
    }

    pub(crate) fn usage_counters(mut self, usage_counters: Arc<UsageCounters>) -> Self {
        self.usage_counters = Some(usage_counters);
        self
    }

    pub(crate) async fn build(self) -> Result<Bucket, ReductError> {
        let name = self.name.expect("Bucket name must be set");
        let data_path = self.data_path.expect("Bucket data path must be set");
        let cfg = self.cfg.expect("Config must be set");
        let io_limiter = self
            .io_limiter
            .unwrap_or_else(|| InFlightIoLimiter::from_cfg(&cfg));
        let usage_counters = self
            .usage_counters
            .unwrap_or_else(|| Arc::new(UsageCounters::default()));
        let settings = Bucket::fill_settings(self.settings.unwrap_or_default(), Bucket::defaults());
        let path = data_path.join(&name);
        let folder_keeper = FolderKeeper::new(path.clone(), &cfg).await;

        let bucket = Bucket {
            name,
            path,
            entries: Arc::new(AsyncRwLock::new(BTreeMap::new())),
            settings: AsyncRwLock::new(settings),
            is_provisioned: AtomicBool::new(false),
            status: AsyncRwLock::new(ResourceStatus::Ready),
            cfg: Arc::new(cfg),
            folder_keeper: Arc::new(folder_keeper),
            queries: AsyncRwLock::new(HashMap::<u64, MultiEntryQuery>::new()),
            io_limiter,
            usage_counters,
        };

        bucket.save_settings().await?;
        Ok(bucket)
    }

    pub(crate) async fn restore(self) -> Result<Bucket, ReductError> {
        let path = self.path.expect("Bucket path must be set");
        let cfg = self.cfg.expect("Config must be set");
        let io_limiter = self
            .io_limiter
            .unwrap_or_else(|| InFlightIoLimiter::from_cfg(&cfg));
        let usage_counters = self
            .usage_counters
            .unwrap_or_else(|| Arc::new(UsageCounters::default()));

        let mut file = FILE_CACHE
            .read(&path.join(SETTINGS_NAME), SeekFrom::Start(0))
            .await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let cfg = Arc::new(cfg);
        let settings = ProtoBucketSettings::decode(&mut Bytes::from(buf))
            .map_err(|e| internal_server_error!("Failed to decode settings: {}", e))?;

        let settings = Bucket::fill_settings(settings.into(), Bucket::defaults());
        let bucket_name = path.file_name().unwrap().to_str().unwrap().to_string();

        let mut entries = BTreeMap::new();
        let mut task_set = Vec::new();
        let folder_keeper = FolderKeeper::new(path.clone(), cfg.as_ref()).await;

        for entry_path in folder_keeper.list_folders().await? {
            let entry_name = normalize_entry_name(
                entry_path
                    .strip_prefix(&path)
                    .unwrap_or(entry_path.as_path()),
            );
            let handler = Entry::restore_with_limiter(
                entry_path,
                entry_name.clone(),
                bucket_name.clone(),
                settings_for_entry(&entry_name, &settings),
                cfg.clone(),
                io_limiter.clone(),
                Arc::clone(&usage_counters),
            );

            task_set.push((entry_name, handler));
        }

        for (entry_name, task) in task_set {
            match task.await {
                Ok(Some(entry)) => {
                    entries.insert(entry.name().to_string(), entry);
                }
                Ok(None) => {}
                Err(err) => {
                    error!(
                        "Failed to restore entry '{}' in bucket '{}': {}",
                        entry_name, bucket_name, err
                    );
                }
            }
        }

        Ok(Bucket {
            name: bucket_name,
            path,
            entries: Arc::new(AsyncRwLock::new(
                entries.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
            )),
            settings: AsyncRwLock::new(settings),
            is_provisioned: AtomicBool::new(false),
            status: AsyncRwLock::new(ResourceStatus::Ready),
            cfg,
            folder_keeper: Arc::new(folder_keeper),
            queries: AsyncRwLock::new(HashMap::<u64, MultiEntryQuery>::new()),
            io_limiter,
            usage_counters,
        })
    }
}
