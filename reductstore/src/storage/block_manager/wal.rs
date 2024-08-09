// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::file_cache::FileCache;
use crate::storage::proto::Record;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

pub(super) enum WalEntry {
    WriteRecord(Record),
    UpdateRecord(Record),
    RemoveBlock,
}

#[async_trait]
pub(super) trait Wal {
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError>;

    async fn read(&self, block_id: u64) -> Result<Option<WalEntry>, ReductError>;

    async fn clean(&self, block_id: u64) -> Result<(), ReductError>;
}

struct WalImpl {
    file_cache: FileCache, // share with block manager??
    root_path: PathBuf,
    path_map: HashMap<u64, PathBuf>,
}

impl WalImpl {
    pub fn new(path_buf: PathBuf) -> Self {
        WalImpl {
            file_cache: FileCache::new(10, Duration::from_secs(60)),
            root_path: path_buf,
            path_map: HashMap::new(),
        }
    }
}

#[async_trait]
impl Wal for WalImpl {
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError> {
        Ok(())
    }

    async fn read(&self, block_id: u64) -> Result<Option<WalEntry>, ReductError> {
        Ok(None)
    }

    async fn clean(&self, block_id: u64) -> Result<(), ReductError> {
        Ok(())
    }
}

pub(in crate::storage) fn create_wal(entry_path: PathBuf) -> Box<dyn Wal + Send + Sync> {
    Box::new(WalImpl::new(entry_path.join("wal")))
}
