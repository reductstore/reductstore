// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub(in crate::storage) fn block_is_compressed(&self, block_id: u64) -> bool {
        self.block_index
            .get_block(block_id)
            .and_then(|block| block.compression)
            .unwrap_or(i32::from(CompressionAlgorithm::None))
            != i32::from(CompressionAlgorithm::None)
    }

    pub fn index_mut(&mut self) -> &mut BlockIndex {
        &mut self.block_index
    }

    pub fn index(&self) -> &BlockIndex {
        &self.block_index
    }

    pub(in crate::storage) fn usage_counters(&self) -> &UsageCounters {
        &self.usage_counters
    }

    pub(in crate::storage) fn usage_counters_arc(&self) -> Arc<UsageCounters> {
        Arc::clone(&self.usage_counters)
    }

    pub(in crate::storage) fn is_replica(&self) -> bool {
        self.cfg.role == InstanceRole::Replica
    }

    pub fn bucket_name(&self) -> &String {
        &self.bucket
    }

    pub fn entry_name(&self) -> &String {
        &self.entry
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
