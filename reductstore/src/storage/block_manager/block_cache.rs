// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::cache::Cache;
use crate::storage::block_manager::BlockRef;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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

    pub fn insert_read(&self, block_id: u64, value: BlockRef) {
        self.read_cache.write().unwrap().insert(block_id, value);
    }

    pub fn insert_write(&self, block_id: u64, value: BlockRef) -> Vec<(u64, BlockRef)> {
        self.write_cache.write().unwrap().insert(block_id, value)
    }

    pub fn get_read(&self, block_id: &u64) -> Option<BlockRef> {
        if let Some(block) = self.write_cache.write().unwrap().get(block_id) {
            return Some(block.clone());
        }

        if let Some(block) = self.read_cache.write().unwrap().get(block_id) {
            return Some(block.clone());
        }

        None
    }

    pub fn get_write(&self, block_id: &u64) -> Option<BlockRef> {
        self.write_cache
            .write()
            .unwrap()
            .get(block_id)
            .map(|v| v.clone())
    }

    pub fn write_values(&self) -> Vec<BlockRef> {
        let mut values = Vec::new();
        for value in self.write_cache.write().unwrap().values() {
            values.push(value.clone());
        }
        values
    }

    #[allow(dead_code)]
    pub fn write_len(&self) -> usize {
        self.write_cache.write().unwrap().len()
    }

    pub fn remove(&self, block_id: &u64) {
        self.write_cache.write().unwrap().remove(block_id);
        self.read_cache.write().unwrap().remove(block_id);
    }

    pub(crate) fn clear(&self) {
        self.write_cache.write().unwrap().clear();
        self.read_cache.write().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::AsyncRwLock;
    use crate::storage::block_manager::block::Block;
    use std::sync::Arc;

    fn block_ref(block_id: u64) -> BlockRef {
        Arc::new(AsyncRwLock::new(Block::new(block_id)))
    }

    #[tokio::test]
    async fn get_read_falls_back_to_read_cache() {
        let cache = BlockCache::new(1, 1, Duration::from_secs(60));
        let block = block_ref(1);

        cache.insert_read(1, block.clone());

        let loaded = cache.get_read(&1);
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().read().await.unwrap().block_id(), 1);
    }
}
