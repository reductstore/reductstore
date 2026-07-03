// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub async fn save_cache_on_disk(&mut self) -> Result<(), ReductError> {
        let blocks = self.block_cache.write_values();
        for block in blocks.iter() {
            {
                let block_id = block.read().await?.block_id();
                self.sync_data_block(block_id).await?;
            }
            self.save_meta_on_disk(block.clone()).await?;
        }

        Ok(())
    }

    pub async fn save_cache_metadata_on_disk(&mut self) -> Result<(), ReductError> {
        let blocks_with_wal = self.wal.list().await.unwrap_or_default();
        if blocks_with_wal.is_empty() {
            return Ok(());
        }

        let blocks = self.block_cache.write_values();
        for block in blocks {
            let block_id = block.read().await?.block_id();
            if blocks_with_wal.contains(&block_id) {
                self.save_meta_on_disk(block).await?;
            }
        }

        Ok(())
    }

    pub async fn save_block(&mut self, block: BlockRef) -> Result<(), ReductError> {
        let id = block.read().await?.block_id();
        for (_, block) in self.block_cache.insert_write(id, block.clone()) {
            self.save_meta_on_disk(block).await?;
        }

        Ok(())
    }

    pub(super) async fn sync_data_block(&self, block_id: u64) -> Result<(), ReductError> {
        if self.cfg.role == InstanceRole::Replica {
            return Ok(());
        }

        let path = self.path_to_data(block_id);
        if !FILE_CACHE.try_exists(&path).await? {
            return Ok(());
        }

        let mut data_block = FILE_CACHE
            .write_or_create(&path, SeekFrom::Current(0))
            .await?;
        data_block.sync_all().await?;
        Ok(())
    }

    // Method save descriptor and update index
    // Note: it calls local sync but not sync_all to avoid blocking entry during synchronization with remote backend
    // the blocks must be synced with backed from FILE_CACHE sync loop
    pub(super) async fn save_meta_on_disk(
        &mut self,
        block_ref: BlockRef,
    ) -> Result<(), ReductError> {
        // Take a snapshot under a short-lived write lock to avoid blocking readers
        let (block_id, block_snapshot) = {
            let block = block_ref.read().await?;
            (block.block_id(), block.to_owned())
        };

        debug!(
            "Saving block {}/{}/{} on disk and updating index",
            self.bucket, self.entry, block_id
        );

        let path = self.path_to_desc(block_id);
        let mut buf = BytesMut::new();

        let mut proto = BlockProto::from(block_snapshot);
        let version = self
            .block_index
            .get_block(block_id)
            .and_then(|block| block.version)
            .unwrap_or(0)
            + 1;
        proto.version = Some(version);
        proto.corrupted = None;
        proto.encode(&mut buf).map_err(|e| {
            internal_server_error!("Failed to encode block descriptor {:?}: {}", path, e)
        })?;
        let len = buf.len() as u64;

        trace!("Writing block descriptor {:?}", path);

        if self.cfg.role != InstanceRole::Replica {
            let mut lock = FILE_CACHE
                .write_or_create(&path, SeekFrom::Start(0))
                .await?;
            lock.set_len(len)?;
            lock.write_all(&buf)?;
            lock.flush_local().await?; // fix https://github.com/reductstore/reductstore/issues/642
        }

        trace!("Updating block index");
        // update index with block crc
        let mut crc = Digest::new();
        crc.write(&buf);
        proto.metadata_size = len; // update metadata size because it changed
        self.block_index
            .insert_or_update_with_crc(proto, crc.sum64());

        if self.cfg.role != InstanceRole::Replica {
            self.block_index.save().await?;

            trace!("Block {}/{}/{} saved", self.bucket, self.entry, block_id);
            // clean WAL
            self.wal.remove(block_id).await?;
        }
        Ok(())
    }
}
