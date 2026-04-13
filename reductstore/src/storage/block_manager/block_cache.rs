// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::cache::Cache;
use crate::storage::block_manager::BlockRef;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

type RefCache = Arc<RwLock<Cache<u64, BlockRef>>>;
type GlobalRefCache = Arc<RwLock<Cache<String, BlockRef>>>;

/// Global read cache shared by all [`BlockCache`] instances in the process.
///
/// Why global:
/// - Entries can be very numerous while only a small subset is hot at any given time.
/// - Per-entry read caches can keep stale/cold entries resident for too long.
/// - A single shared cache gives unified memory pressure and eviction across entries.
static GLOBAL_READ_CACHE: OnceLock<GlobalRefCache> = OnceLock::new();

/// Two-level block cache for an entry:
/// - `write_cache` is local and keyed by `block_id` only.
/// - `read_cache` is global and keyed by `namespace::block_id`.
///
/// The local write cache keeps write-path semantics isolated per entry.
/// The global read cache improves memory behavior when many entries exist,
/// allowing stale/cold entry blocks to be evicted under shared pressure.
pub(super) struct BlockCache {
    /// Unique entry namespace (typically entry path) used to build global read keys.
    namespace: String,
    /// Per-entry write cache. Kept local to avoid cross-entry write interactions.
    write_cache: RefCache,
    /// Process-wide read cache shared across entries.
    read_cache: GlobalRefCache,
}

impl BlockCache {
    pub fn new(
        namespace: String,
        write_size: usize,
        read_size: usize,
        ttl: Duration,
    ) -> BlockCache {
        let read_cache = GLOBAL_READ_CACHE
            .get_or_init(|| Arc::new(RwLock::new(Cache::new(read_size, ttl))))
            .clone();

        BlockCache {
            namespace,
            write_cache: Arc::new(RwLock::new(Cache::new(write_size, ttl))),
            read_cache,
        }
    }

    pub fn insert_read(&self, block_id: u64, value: BlockRef) {
        self.read_cache
            .write()
            .unwrap()
            .insert(self.read_key(block_id), value);
    }

    pub fn insert_write(&self, block_id: u64, value: BlockRef) -> Vec<(u64, BlockRef)> {
        self.write_cache.write().unwrap().insert(block_id, value)
    }

    pub fn get_read(&self, block_id: &u64) -> Option<BlockRef> {
        if let Some(block) = self.write_cache.write().unwrap().get(block_id) {
            return Some(block.clone());
        }

        if let Some(block) = self
            .read_cache
            .write()
            .unwrap()
            .get(&self.read_key(*block_id))
        {
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
        self.read_cache
            .write()
            .unwrap()
            .remove(&self.read_key(*block_id));
    }

    pub(crate) fn clear(&self) {
        self.write_cache.write().unwrap().clear();
        let mut read_cache = self.read_cache.write().unwrap();
        let prefix = self.read_key_prefix();
        let keys_to_remove = read_cache
            .keys()
            .iter()
            .filter(|key| key.starts_with(&prefix))
            .map(|key| (*key).clone())
            .collect::<Vec<_>>();

        for key in keys_to_remove {
            read_cache.remove(&key);
        }
    }

    fn read_key(&self, block_id: u64) -> String {
        format!("{}::{}", self.namespace, block_id)
    }

    fn read_key_prefix(&self) -> String {
        format!("{}::", self.namespace)
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
        let cache = BlockCache::new("bucket/entry".to_string(), 1, 1, Duration::from_secs(60));
        let block = block_ref(1);

        cache.insert_read(1, block.clone());

        let loaded = cache.get_read(&1);
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().read().await.unwrap().block_id(), 1);
    }
}
