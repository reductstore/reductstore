// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::{strategy_for_entry, Entry, EntrySettings};
use crate::cfg::Cfg;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
use crate::storage::entry::entry_loader::EntryLoader;
use crate::storage::in_flight::InFlightIoLimiter;
use crate::storage::usage::UsageCounters;
use reduct_base::error::ReductError;
use reduct_base::msg::status::ResourceStatus;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct EntryBuilder {
    name: Option<String>,
    bucket_name: Option<String>,
    bucket_path: Option<PathBuf>,
    path: Option<PathBuf>,
    settings: Option<EntrySettings>,
    cfg: Option<Arc<Cfg>>,
    io_limiter: Option<InFlightIoLimiter>,
    usage_counters: Option<Arc<UsageCounters>>,
}

impl EntryBuilder {
    pub(crate) fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    pub(crate) fn bucket_path(mut self, bucket_path: PathBuf) -> Self {
        self.bucket_path = Some(bucket_path);
        self
    }

    pub(crate) fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub(crate) fn settings(mut self, settings: EntrySettings) -> Self {
        self.settings = Some(settings);
        self
    }

    pub(crate) fn cfg(mut self, cfg: Arc<Cfg>) -> Self {
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

    pub(crate) async fn build(self) -> Result<Entry, ReductError> {
        let name = self.name.expect("Entry name must be set");
        let bucket_path = self.bucket_path.expect("Bucket path must be set");
        let bucket_name = bucket_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let path = bucket_path.join(&name);
        let settings = self.settings.expect("Entry settings must be set");
        let cfg = self.cfg.expect("Config must be set");
        let io_limiter = self
            .io_limiter
            .unwrap_or_else(|| InFlightIoLimiter::from_cfg(cfg.as_ref()));
        let usage_counters = self
            .usage_counters
            .unwrap_or_else(|| Arc::new(UsageCounters::default()));
        let block_index_path = path.join(BLOCK_INDEX_FILE);
        let block_index = BlockIndex::new(block_index_path.clone());

        if !FILE_CACHE.try_exists(&block_index_path).await? {
            FILE_CACHE.create_dir_all(&path).await?;
            block_index.save().await?;
        }

        Ok(Entry {
            name: name.clone(),
            bucket_name: bucket_name.clone(),
            settings: AsyncRwLock::new(settings),
            block_manager: Arc::new(AsyncRwLock::new(
                BlockManager::build(
                    path.clone(),
                    block_index,
                    bucket_name,
                    name.clone(),
                    cfg.clone(),
                    usage_counters,
                )
                .await?,
            )),
            system_behavior: strategy_for_entry(&name),
            queries: Arc::new(AsyncRwLock::new(HashMap::new())),
            status: AsyncRwLock::new(ResourceStatus::Ready),
            path,
            cfg,
            io_limiter,
        })
    }

    pub(crate) async fn restore(self) -> Result<Option<Entry>, ReductError> {
        let path = self.path.expect("Entry path must be set");
        let entry_name = self
            .name
            .unwrap_or_else(|| path.file_name().unwrap().to_str().unwrap().to_string());
        let bucket_name = self.bucket_name.unwrap_or_else(|| {
            path.parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        });
        let settings = self.settings.expect("Entry settings must be set");
        let cfg = self.cfg.expect("Config must be set");
        let io_limiter = self
            .io_limiter
            .unwrap_or_else(|| InFlightIoLimiter::from_cfg(cfg.as_ref()));
        let usage_counters = self
            .usage_counters
            .unwrap_or_else(|| Arc::new(UsageCounters::default()));

        EntryLoader::restore_entry_with_names(
            path,
            entry_name,
            bucket_name,
            settings,
            cfg,
            io_limiter,
            usage_counters,
        )
        .await
    }
}
