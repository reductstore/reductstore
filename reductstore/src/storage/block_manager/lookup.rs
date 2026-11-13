// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;
use reduct_base::not_found;

impl BlockManager {
    pub async fn find_block_candidate(&mut self, start: u64) -> Result<BlockRef, ReductError> {
        let id = self.find_block_id(start)?;
        self.load_block(id).await
    }

    /// Finds the block which contains a record with the exact timestamp.
    ///
    /// A belated write can create a block whose time range overlaps an earlier block. Search
    /// predecessor blocks from newest to oldest rather than relying on the index floor lookup.
    pub async fn find_block_with_record(&mut self, time: u64) -> Result<BlockRef, ReductError> {
        let block_ref = self.find_block_candidate(time).await?;
        let (block_id, has_record) = {
            let block = block_ref.read().await?;
            (block.block_id(), block.has_record(time))
        };

        if has_record {
            return Ok(block_ref);
        }

        // Only load predecessors whose persisted or live range can contain the record.
        let block_ids = self
            .block_index
            .last_record_index()
            .range(..block_id)
            .rev()
            .filter_map(|(&id, &latest)| {
                (!self.block_index.is_corrupted(id)
                    && (time <= latest || self.block_cache.get_read(&id).is_some()))
                .then_some(id)
            })
            .collect::<Vec<_>>();

        for block_id in block_ids {
            let block_ref = self.load_block(block_id).await?;
            let block = block_ref.read().await?;

            if block.has_record(time) {
                drop(block);
                return Ok(block_ref);
            }
        }

        Err(not_found!(
            "Record {} not found in block {}/{}/{}",
            time,
            self.bucket,
            self.entry,
            block_id
        ))
    }

    fn find_block_id(&self, start: u64) -> Result<u64, ReductError> {
        self.block_index
            .block_ids()
            .range(..=start)
            .rev()
            .find(|&&id| !self.block_index.is_corrupted(id))
            .copied()
            .ok_or_else(|| {
                ReductError::not_found(&format!(
                    "Record {} not found in entry {}/{}",
                    start, self.bucket, self.entry
                ))
            })
    }

    pub fn find_cached_block(&self, start: u64) -> Option<BlockRef> {
        let id = self.find_block_id(start).ok()?;

        self.block_cache.get_read(&id)
    }

    pub async fn load_block(&mut self, block_id: u64) -> Result<BlockRef, ReductError> {
        // first check if we have the block in write cache
        let mut cached_block = self.block_cache.get_read(&block_id);
        if cached_block.is_none() {
            let path = self.resolve_desc_path(block_id).await?;
            let buf = match FILE_CACHE.read(&path, SeekFrom::Start(0)).await {
                Ok(mut file) => {
                    let mut buf = vec![];
                    file.read_to_end(&mut buf)?;
                    buf
                }
                Err(err) => {
                    // Re-check existence to distinguish a transient TOCTOU race
                    // (descriptor removed after the first check) from a real read failure.
                    if self.cfg.role == InstanceRole::Replica
                        && !FILE_CACHE.try_exists(&path).await?
                    {
                        self.block_index.remove_block(block_id);
                        return Err(too_early!(
                            "Block descriptor {:?} can't be read on replica yet: {}. Reload index and retry",
                            path,
                            err
                        ));
                    }

                    // here we can't read the block descriptor, it might be corrupted or not exist
                    let err_msg = format!("Block descriptor {:?} can't be read: {}", path, err);
                    error!("{}", &err_msg);
                    self.mark_block_corrupted(block_id).await?;
                    return Err(internal_server_error!(&err_msg));
                }
            };

            // calculate crc of the block descriptor
            let mut crc = Digest::new();
            crc.write(&buf);

            if let Some(block) = self.block_index.get_block(block_id) {
                if let Some(block_crc) = block.crc64 {
                    // we check crc if the crc is stored in the index for backward compatibility
                    if block_crc != crc.sum64() {
                        if self.cfg.role == InstanceRole::Replica {
                            warn!(
                                "Block descriptor {:?} CRC mismatch on replica: index CRC {} mismatch with calculated CRC {}. Treat as transient and reload index",
                                path,
                                block_crc,
                                crc.sum64()
                            );
                            self.invalidate_replica_block_cache(block_id).await?;
                            self.block_index.remove_block(block_id);
                            return Err(too_early!(
                                "Block descriptor {:?} CRC mismatch on replica. Reload index and retry",
                                path
                            ));
                        }

                        match BlockProto::decode(Bytes::from(buf.clone())) {
                            Ok(decoded) => {
                                let desc_version = decoded.version.unwrap_or(0);
                                let index_version = block.version.unwrap_or(0);

                                if desc_version > index_version {
                                    warn!(
                                        "Block descriptor {:?} is newer than index (v{} > v{}). Updating index",
                                        path, desc_version, index_version
                                    );

                                    self.block_index
                                        .insert_or_update_with_crc(decoded.clone(), crc.sum64());
                                    self.block_index.save().await?;
                                } else {
                                    error!(
                                        "Block descriptor {:?} is corrupted: index CRC {} mismatch with calculated CRC {}, version {} <= {}",
                                        path,
                                        block_crc,
                                        crc.sum64(),
                                        desc_version,
                                        index_version
                                    );

                                    self.mark_block_corrupted(block_id).await?;
                                    return Err(internal_server_error!(
                                        "Block descriptor {:?} is corrupted",
                                        path
                                    ));
                                }
                            }
                            Err(err) => {
                                error!(
                                    "Block descriptor {:?} is corrupted: index CRC {} mismatch with calculated CRC {} and descriptor can't be decoded: {}",
                                    path,
                                    block_crc,
                                    crc.sum64(),
                                    err
                                );
                                self.mark_block_corrupted(block_id).await?;
                                return Err(internal_server_error!(
                                    "Block descriptor {:?} is corrupted",
                                    path
                                ));
                            }
                        }
                    }
                }
            } else {
                return Err(internal_server_error!(
                    "Block descriptor {:?} is not in the index",
                    path
                ));
            }

            // parse the block descriptor
            let mut block_from_disk = match BlockProto::decode(Bytes::from(buf)) {
                Ok(block) => block,
                Err(e) => {
                    self.mark_block_corrupted(block_id).await?;
                    return Err(internal_server_error!(
                        "Failed to decode block descriptor {:?}: {}",
                        path,
                        e
                    ));
                }
            };

            if block_from_disk.begin_time.is_none() {
                warn!(
                    "Block descriptor {:?} has no begin time. It might be recovered from the WAL",
                    path
                );
                block_from_disk.begin_time = Some(us_to_ts(&block_id));
            }

            cached_block = Some(Arc::new(AsyncRwLock::new(block_from_disk.into())));
        }

        let cached_block = cached_block.unwrap();
        self.block_cache.insert_read(block_id, cached_block.clone());
        Ok(cached_block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_manager::test_utils::{block_id, block_manager};
    use crate::storage::block_manager::BlockManager;
    use crate::storage::proto::{Block as BlockProto, Record};
    use prost::Message;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

    #[fixture]
    async fn predecessor_blocks(#[future] block_manager: BlockManager) -> (BlockManager, BlockRef) {
        let mut bm = block_manager.await;
        let older = bm.start_new_block(10, 1024).await.unwrap();
        bm.start_new_block(30, 1024).await.unwrap();
        (bm, older)
    }

    fn record(time: u64) -> Record {
        Record {
            timestamp: Some(us_to_ts(&time)),
            begin: 0,
            end: 1,
            state: 1,
            labels: vec![],
            content_type: String::new(),
        }
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn missing_record_skips_predecessor_descriptors(
        #[future] predecessor_blocks: (BlockManager, BlockRef),
        #[case] corrupted: bool,
    ) {
        let (mut bm, older) = predecessor_blocks.await;
        older
            .write()
            .await
            .unwrap()
            .insert_or_update_record(record(if corrupted { 40 } else { 12 }));
        bm.save_meta_on_disk(older).await.unwrap();
        if corrupted {
            bm.mark_block_corrupted(10).await.unwrap();
        }
        bm.block_cache.remove(&10);
        FILE_CACHE.remove(&bm.path_to_desc(10)).await.unwrap();

        let err = bm.find_block_with_record(31).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::NotFound);
    }

    #[rstest]
    #[tokio::test]
    async fn finds_record_in_older_overlapping_block(
        #[future] predecessor_blocks: (BlockManager, BlockRef),
    ) {
        let (mut bm, older) = predecessor_blocks.await;
        older
            .write()
            .await
            .unwrap()
            .insert_or_update_record(record(40));
        bm.save_meta_on_disk(older).await.unwrap();
        bm.block_cache.remove(&10);

        let found = bm.find_block_with_record(40).await.unwrap();
        assert_eq!(found.read().await.unwrap().block_id(), 10);
    }

    #[rstest]
    #[tokio::test]
    async fn finds_record_in_cached_block_with_stale_index(
        #[future] predecessor_blocks: (BlockManager, BlockRef),
    ) {
        let (mut bm, older) = predecessor_blocks.await;
        older
            .write()
            .await
            .unwrap()
            .insert_or_update_record(record(40));
        assert_eq!(bm.index().last_record_index().get(&10), Some(&0));

        let found = bm.find_block_with_record(40).await.unwrap();
        assert_eq!(found.read().await.unwrap().block_id(), 10);

        bm.save_block(older).await.unwrap();
        assert_eq!(bm.index().last_record_index().get(&10), Some(&40));
    }

    #[rstest]
    #[tokio::test]
    async fn test_loading_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        block_manager.start_new_block(block_id, 1024).await.unwrap();
        let block_ref = block_manager.start_new_block(20000005, 1024).await.unwrap();
        let block = block_ref.read().await.unwrap();
        let loaded_block = block_manager.load_block(block.block_id()).await.unwrap();
        assert_eq!(
            loaded_block.read().await.unwrap().block_id(),
            block.block_id()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_loading_corrupted_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        block_manager.start_new_block(block_id, 1024).await.unwrap();
        block_manager.save_cache_on_disk().await.unwrap();
        block_manager.block_cache.remove(&block_id);

        let path = block_manager.path_to_desc(block_id);
        std::fs::write(&path, b"corrupted").unwrap();

        let err = block_manager.load_block(block_id).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(err.to_string().contains("corrupted"));
        assert!(block_manager.index().get_block(block_id).is_some());
        assert!(block_manager.is_block_corrupted(block_id));
    }

    #[rstest]
    #[tokio::test]
    async fn test_load_block_stale_index_self_heals(#[future] block_manager: BlockManager) {
        let mut block_manager = block_manager.await;
        let block_id = 1;

        let block_ref = block_manager.load_block(block_id).await.unwrap();
        block_manager.save_meta_on_disk(block_ref).await.unwrap();
        block_manager.block_cache.remove(&block_id);
        let desc_version = BlockProto::decode(
            std::fs::read(block_manager.path_to_desc(block_id))
                .unwrap()
                .as_slice(),
        )
        .unwrap()
        .version
        .unwrap();

        let index_block = block_manager.index_mut().get_block_mut(block_id).unwrap();
        index_block.version = Some(desc_version - 1);
        index_block.crc64 = Some(0);
        block_manager.index_mut().save().await.unwrap();

        let loaded = block_manager.load_block(block_id).await;

        assert!(loaded.is_ok());
        let index_block = block_manager.index().get_block(block_id).unwrap();
        assert_eq!(index_block.version, Some(desc_version));
        assert_ne!(index_block.crc64, Some(0));
    }

    #[rstest]
    #[tokio::test]
    async fn test_load_block_corrupted_descriptor_detected(#[future] block_manager: BlockManager) {
        let mut block_manager = block_manager.await;
        let block_id = 1;
        block_manager.block_cache.remove(&block_id);

        let path = block_manager.path_to_desc(block_id);
        let mut block_proto = BlockProto::decode(std::fs::read(&path).unwrap().as_slice()).unwrap();
        block_proto.record_count += 1;
        std::fs::write(&path, block_proto.encode_to_vec()).unwrap();

        let err = block_manager.load_block(block_id).await.err().unwrap();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(block_manager.is_block_corrupted(block_id));
    }

    #[rstest]
    #[tokio::test]
    async fn test_recover_being_time_from_id(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        block_manager.start_new_block(block_id, 1024).await.unwrap();
        block_manager.block_cache.remove(&block_id);

        let path = block_manager.path_to_desc(block_id);
        std::fs::write(&path, b"").unwrap();

        let result = block_manager.load_block(block_id).await;
        assert!(
            result.is_ok(),
            "It's ok to recover begin time from block id for blocks which aren't synced yet"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_reading(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start_new_block(block_id, 1024).await.unwrap();
        let block_id = block.read().await.unwrap().block_id();
        let loaded_block = block_manager.load_block(block_id).await.unwrap();
        assert_eq!(loaded_block.read().await.unwrap().block_id(), block_id);
    }

    #[rstest]
    #[tokio::test]
    async fn test_recovering_index_if_no_meta_file(
        #[future] block_manager: BlockManager,
        block_id: u64,
    ) {
        let mut block_manager = block_manager.await;
        assert!(block_manager.index().get_block(block_id).is_some());

        FILE_CACHE
            .remove(&block_manager.path_to_desc(block_id))
            .await
            .unwrap();
        block_manager.block_cache.remove(&block_id); // remove block from cache to load it from disk
        assert_eq!(
            block_manager
                .load_block(block_id)
                .await
                .err()
                .unwrap()
                .status(),
            ErrorCode::InternalServerError
        );
        assert!(
            block_manager.index().get_block(block_id).is_some(),
            "corrupted blocks remain in the index for quota cleanup"
        );
        assert!(block_manager.is_block_corrupted(block_id));
    }
}
