// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::cache::Cache;
use crate::storage::block_manager::BlockRef;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

type RefCache = Arc<RwLock<Cache<u64, BlockRef>>>;
pub(super) struct BlockCache {
    write_cache: RefCache,
    read_cache: RefCache,
}

impl BlockCache {
    pub fn new(write_size: usize, read_size: usize, ttl: Duration) -> BlockCache {
        BlockCache {
            write_cache: Arc::new(RwLock::new(Cache::new(write_size, ttl))),
            read_cache: Arc::new(RwLock::new(Cache::new(read_size, ttl))),
        }
    }

    pub async fn insert_read(&self, block_id: u64, value: BlockRef) {
        self.read_cache.write().await.insert(block_id, value);
    }

    pub async fn insert_write(&self, block_id: u64, value: BlockRef) -> Vec<(u64, BlockRef)> {
        self.write_cache.write().await.insert(block_id, value)
    }

    pub async fn get_read(&self, block_id: &u64) -> Option<BlockRef> {
        let mut cached_block = None;
        if let Some(block) = self.write_cache.write().await.get(block_id) {
            if &block.read().await.block_id() == block_id {
                cached_block = Some(block.clone());
            } else if let Some(block) = self.read_cache.write().await.get(block_id) {
                // then check if we have the block in read cache
                if &block.read().await.block_id() == block_id {
                    cached_block = Some(block.clone());
                }
            }
        }

        cached_block
    }

    pub async fn get_write(&self, block_id: &u64) -> Option<BlockRef> {
        self.write_cache
            .write()
            .await
            .get(block_id)
            .map(|v| v.clone())
    }

    pub async fn write_values(&self) -> Vec<BlockRef> {
        let mut values = Vec::new();
        for value in self.write_cache.write().await.values() {
            values.push(value.clone());
        }
        values
    }

    pub async fn write_len(&self) -> usize {
        self.write_cache.write().await.len()
    }

    pub async fn remove(&self, block_id: &u64) {
        self.write_cache.write().await.remove(block_id);
        self.read_cache.write().await.remove(block_id);
    }
}
