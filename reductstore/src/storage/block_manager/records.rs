// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    /// Update records in a block and save it on disk.
    ///
    /// # Arguments
    ///
    /// * `block_id` - Block to update records in.
    /// * `records` - Records to update.
    ///
    ///
    /// # Panics
    ///
    /// * If the record is not in the block.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the records were updated successfully.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If failed to append to WAL or save the block on disk.
    pub async fn update_records(
        &mut self,
        block_id: u64,
        records: Vec<Record>,
    ) -> Result<(), ReductError> {
        self.decompress_block(block_id).await?;
        let block_ref = self.load_block(block_id).await?;

        // First, append all WAL entries
        for record in records.iter() {
            self.wal
                .append(block_id, WalEntry::UpdateRecord(record.clone()))
                .await?;
        }

        // Then, update the block in-memory (no await needed here)
        {
            let mut block = block_ref.write().await?;
            for record in records.into_iter() {
                block.insert_or_update_record(record);
            }
        }

        self.save_block(block_ref).await
    }

    /// Remove records from a block and save it on disk.
    ///
    /// The method will create a temporary block file and copy the retained records to it.
    /// In the end, the original block file will be replaced with the temporary one and the block descriptor will be updated.
    ///
    /// # Arguments
    ///
    /// * `block_id` - Block to remove records from.
    /// * `records` - Record timestamps to remove.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the records were removed successfully.
    pub async fn remove_records(
        &mut self,
        block_id: u64,
        records: Vec<u64>,
    ) -> Result<(), ReductError> {
        self.decompress_block(block_id).await?;
        let block_ref = self.load_block(block_id).await?;

        {
            let mut block = block_ref.write().await?;
            for record_time in records {
                block.remove_record(record_time);
                self.wal
                    .append(block_id, WalEntry::RemoveRecord(record_time))
                    .await?;
            }

            // if the block is empty, remove it
            if block.record_count() == 0 {
                let block_id = block.block_id();
                drop(block);
                return self.remove_block(block_id).await;
            }
        }

        // Collect record info needed for copying
        let (temp_block_path, record_info, src_block_path) = {
            let block = block_ref.read().await?;
            let temp_block_path = self.path.join(format!("{}.blk.tmp", block.block_id()));
            let src_block_path = self.path_to_data(block.block_id());

            // Collect record positions
            let record_info: Vec<(u64, u64, u64)> = block
                .record_index()
                .values()
                .map(|record| {
                    let record_time = ts_to_us(&record.timestamp.unwrap());
                    (record_time, record.begin, record.end - record.begin)
                })
                .collect();

            (temp_block_path, record_info, src_block_path)
        };

        // create a temporary block file outside of the cache to avoid unnecessary evictions
        let mut temp_block = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&temp_block_path)
            .map_err(|e| {
                internal_server_error!(
                    "Failed to create temporary block file {:?}: {}",
                    temp_block_path,
                    e
                )
            })?;

        // Copy records to temp file and track new positions
        let mut new_positions: Vec<(u64, u64, u64)> = Vec::new();
        let mut total_offset = 0u64;

        for (record_time, begin, record_size) in record_info {
            let mut read_bytes = 0;
            while read_bytes < record_size {
                let (buf, read) =
                    read_in_chunks(&src_block_path, begin, record_size, read_bytes).await?;

                read_bytes += read as u64;
                temp_block.write_all(&buf)?;
            }

            let new_begin = total_offset;
            total_offset += read_bytes;
            let new_end = total_offset;

            new_positions.push((record_time, new_begin, new_end));

            trace!(
                "Record {}/{} retained with new position begin={}, end={}",
                block_id,
                record_time,
                new_begin,
                new_end
            );
        }

        // Update record positions in the block
        {
            let mut block = block_ref.write().await?;
            for (record_time, new_begin, new_end) in new_positions {
                if let Some(record) = block.record_index_mut().get_mut(&record_time) {
                    record.begin = new_begin;
                    record.end = new_end;
                }
            }

            debug!(
                "New block {:?} is created with {} records",
                temp_block_path,
                block.record_count()
            );
        }

        // Replace the old block file with the new one
        let block_path = {
            let block = block_ref.read().await?;
            self.path_to_data(block.block_id())
        };

        tokio::fs::remove_file(&block_path).await?;
        tokio::fs::rename(&temp_block_path, &block_path).await?;

        FILE_CACHE.discard_recursive(&block_path).await?;
        let mut block_file = FILE_CACHE
            .write_or_create(&block_path, SeekFrom::Start(0))
            .await?;
        block_file.sync_all().await?;

        self.save_meta_on_disk(block_ref).await
    }

    /// Begin writing a record to a block.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to write to.
    /// * `record_timestamp` - Timestamp of the record to write.
    ///
    /// # Returns
    ///
    /// * `Ok(file)` - File to write to.
    pub fn begin_write_record(
        &self,
        block: &Block,
        record_timestamp: u64,
    ) -> Result<(PathBuf, u64), ReductError> {
        let path = self.path_to_data(block.block_id());
        let offset = block.get_record(record_timestamp).unwrap().begin;
        Ok((path, offset))
    }

    /// Finish writing a record to a block.
    ///
    /// This method will update the record state, add WAL entry and save the block on disk if needed.
    ///
    /// # Arguments
    ///
    /// * `block_id` - ID of the block to write to.
    /// * `state` - State of the record.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If file system operation failed.
    pub async fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_timestamp: u64,
    ) -> Result<(), ReductError> {
        // check if the block is still in cache
        let block_ref = if let Some(block_ref) = self.block_cache.get_write(&block_id) {
            let block = block_ref.read().await?;
            if block.block_id() == block_id {
                block_ref.clone()
            } else {
                self.load_block(block_id).await?
            }
        } else {
            self.load_block(block_id).await?
        };

        // Get the record for WAL entry before modifying
        let wal_record = {
            let mut block = block_ref.write().await?;
            block.change_record_state(record_timestamp, i32::from(state))?;
            block.get_record(record_timestamp).unwrap().clone()
        };

        // write to WAL (no guard held)
        self.wal
            .append(block_id, WalEntry::WriteRecord(wal_record))
            .await?;

        self.save_block(block_ref).await?;

        debug!(
            "Finished writing record {} to block {}/{}/{}.meta with state {:?}",
            record_timestamp, self.bucket, self.entry, block_id, state
        );

        Ok(())
    }

    /// Begin reading a record from a block.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to read from.
    /// * `record_timestamp` - Timestamp of the record to read.
    ///
    /// # Returns
    ///
    /// * `Ok(file, offset)` - File to read from and offset to start reading.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If file system operation failed.
    pub(crate) async fn begin_read_record(
        &self,
        block: &Block,
        record_timestamp: u64,
    ) -> Result<(PathBuf, u64), ReductError> {
        let path = self.resolve_data_path(block.block_id()).await?;
        let offset = block.get_record(record_timestamp).unwrap().begin;
        Ok((path, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::AsyncRwLock;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::block_manager::compress::CompressionAlgorithm;
    use crate::storage::block_manager::test_utils::{block, block_id, block_manager, write_record};
    use crate::storage::block_manager::wal::WalEntry;
    use crate::storage::block_manager::{BlockManager, BlockRef, BLOCK_INDEX_FILE};
    use crate::storage::engine::MAX_IO_BUFFER_SIZE;
    use crate::storage::entry::{RecordReader, RecordWriter};
    use crate::storage::proto::record::Label;
    use crate::storage::proto::{ts_to_us, BlockIndex as BlockIndexProto, Record};
    use prost::bytes::{Bytes, BytesMut};
    use prost::Message;
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::{ReadRecord, WriteRecord};
    use rstest::rstest;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    #[rstest]
    #[tokio::test]
    async fn test_unfinished_writing(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let block_manager = block_manager.await;
        let block = block.await;
        let block_manager = Arc::new(AsyncRwLock::new(block_manager));
        let mut writer = RecordWriter::try_new(Arc::clone(&block_manager), block, 0)
            .await
            .unwrap();

        writer.send(Ok(None)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await; // wait for thread to finish

        let block_ref = block_manager
            .write()
            .await
            .unwrap()
            .load_block(block_id)
            .await
            .unwrap();
        assert_eq!(
            block_ref.read().await.unwrap().get_record(0).unwrap().state,
            2
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_index_when_start_new_one(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let block_manager = block_manager.await;
        let block = block.await;
        let block_manager = Arc::new(AsyncRwLock::new(block_manager));
        let record = Record {
            timestamp: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            begin: 0,
            end: 5,
            state: 1,
            labels: vec![],
            content_type: "".to_string(),
        };
        block
            .write()
            .await
            .unwrap()
            .insert_or_update_record(record.clone());

        let mut writer = RecordWriter::try_new(Arc::clone(&block_manager), block, 1000_000)
            .await
            .unwrap();
        writer.send(Ok(Some(Bytes::from("hallo")))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await; // wait for thread to finish

        // must save record in WAL
        let mut bm = block_manager.write().await.unwrap();
        {
            let entries = bm.wal.read(block_id).await.unwrap();
            assert_eq!(entries.len(), 1);
            let record_from_wall = match &entries[0] {
                WalEntry::WriteRecord(record) => record,
                _ => panic!("Expected WriteRecord"),
            };

            assert_eq!(record, *record_from_wall);
        }

        let index = BlockIndex::from_proto(
            PathBuf::new(),
            BlockIndexProto::decode(
                std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            index.get_block(block_id).unwrap().record_count,
            1,
            "index not updated"
        );

        // drop cache in disk when block is changed (we need two blocks because of cache)
        let _ = bm.start_new_block(block_id + 1, 1024).await.unwrap();
        let _ = bm.start_new_block(block_id + 2, 1024).await.unwrap();

        let err = bm.wal.read(block_id).await.err().unwrap();
        assert_eq!(
            err.status(),
            ErrorCode::InternalServerError,
            "WAL removed after index updated"
        );

        let index = BlockIndex::from_proto(
            PathBuf::new(),
            BlockIndexProto::decode(
                std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            index.get_block(block_id).unwrap().record_count,
            2,
            "index update"
        );

        let er = bm.wal.read(block_id + 1).await.err().unwrap();
        assert_eq!(
            er.status(),
            ErrorCode::InternalServerError,
            "WAL not removed after index updated"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_index_when_update_labels(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let mut bm = block_manager.await;
        let block = block.await;
        let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        assert_eq!(
            index.get_block(block_id).unwrap().metadata_size,
            27,
            "index not updated"
        );

        let block_ref = block;
        let record = {
            let lock = block_ref.write().await.unwrap();
            let mut record = lock.get_record(0).unwrap().clone();
            record.labels = vec![Label {
                name: "key".to_string(),
                value: "value".to_string(),
            }];

            record
        };

        bm.update_records(block_id, vec![record]).await.unwrap();
        bm.save_cache_on_disk().await.unwrap();
        let block_index_proto = BlockIndexProto::decode(
            std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(
            block_index_proto.blocks[0].metadata_size, 41,
            "index updated"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_records_decompresses_block(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let mut bm = block_manager.await;
        let block_ref = block.await;
        bm.compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        let record = {
            let block = block_ref.read().await.unwrap();
            let mut record = block.get_record(0).unwrap().clone();
            record.labels = vec![Label {
                name: "key".to_string(),
                value: "value".to_string(),
            }];
            record
        };

        bm.update_records(block_id, vec![record]).await.unwrap();

        assert!(!bm.block_is_compressed(block_id));
        assert!(bm.path_to_data(block_id).exists());
        assert!(bm.path_to_desc(block_id).exists());
        assert!(!bm.path_to_compressed_data(block_id).exists());
        assert!(!bm.path_to_compressed_desc(block_id).exists());
        let block_ref = bm.load_block(block_id).await.unwrap();
        let block = block_ref.read().await.unwrap();
        assert_eq!(block.get_record(0).unwrap().labels[0].value, "value");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_index_when_remove_record(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let block_manager = block_manager.await;
        let block = block.await;
        let block_manager = Arc::new(AsyncRwLock::new(block_manager));
        write_record(1, 100, &block_manager, block.clone()).await;

        let mut bm = block_manager.write().await.unwrap();
        let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        assert_eq!(index.get_block(1).unwrap().record_count, 2);

        bm.remove_records(block_id, vec![1]).await.unwrap();

        let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        assert_eq!(index.get_block(1).unwrap().record_count, 1, "index updated");
    }

    #[rstest]
    #[case(0)]
    #[case(500)]
    #[case(MAX_IO_BUFFER_SIZE+1)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_records(
        #[case] record_size: usize,
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let block_manager = block_manager.await;
        let block = block.await;
        let block_manager = Arc::new(AsyncRwLock::new(block_manager));
        let block_ref = block;
        let (record, record_body) =
            write_record(1, record_size, &block_manager, block_ref.clone()).await;

        // remove first record
        block_manager
            .write()
            .await
            .unwrap()
            .remove_records(block_id, vec![0])
            .await
            .unwrap();

        let block = block_ref.read().await.unwrap();
        assert_eq!(block.record_count(), 1);

        let record_time = ts_to_us(&record.timestamp.unwrap());
        let record = block.get_record(record_time).unwrap();
        assert_eq!(ts_to_us(&record.timestamp.unwrap()), record_time);
        assert_eq!(record.begin, 0);
        assert_eq!(record.end, record_size as u64);
        assert_eq!(record.state, 1);

        // read content
        let mut reader = RecordReader::try_new(
            Arc::clone(&block_manager),
            block_ref.clone(),
            record_time,
            None,
            None,
        )
        .await
        .unwrap();

        let mut received = BytesMut::new();
        while let Some(Ok(chunk)) = reader.read_chunk() {
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received.len(), record_size);
        assert_eq!(received, record_body.as_bytes());

        assert!(
            block_manager
                .write()
                .await
                .unwrap()
                .wal
                .read(block.block_id())
                .await
                .is_err(),
            "wal must be removed after successful update"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_only_one_record(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        block_manager
            .remove_records(block_id, vec![0])
            .await
            .unwrap();

        // block must be removed
        let err = block_manager.load_block(block_id).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(block_manager.block_index.get_block(block_id).is_none());
        assert!(!block_manager.exist(block_id).await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_records_wal(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
    ) {
        let block_manager = block_manager.await;
        let block = block.await;
        let block_manager = Arc::new(AsyncRwLock::new(block_manager));
        write_record(1, 5, &block_manager, block.clone()).await;

        let mut bm = block_manager.write().await.unwrap();

        let block_id = block.read().await.unwrap().block_id();
        FILE_CACHE.remove(&bm.path_to_data(block_id)).await.unwrap();

        let res = bm.remove_records(block_id, vec![1]).await;
        assert!(
            res.is_err(),
            "we broke the method removing the source block"
        );

        let entries = bm.wal.read(block_id).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            WalEntry::RemoveRecord(1),
            "wal must have the record"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_records_decompresses_block(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let block_manager = block_manager.await;
        let block = block.await;
        let block_manager = Arc::new(AsyncRwLock::new(block_manager));
        write_record(1, 10, &block_manager, block.clone()).await;

        {
            let mut bm = block_manager.write().await.unwrap();
            bm.compress_block(block_id, CompressionAlgorithm::Zstd)
                .await
                .unwrap();
            bm.remove_records(block_id, vec![0]).await.unwrap();

            assert!(!bm.block_is_compressed(block_id));
            assert!(bm.path_to_data(block_id).exists());
            assert!(bm.path_to_desc(block_id).exists());
            assert!(!bm.path_to_compressed_data(block_id).exists());
            assert!(!bm.path_to_compressed_desc(block_id).exists());
        }

        let block = block.read().await.unwrap();
        assert_eq!(block.record_count(), 1);
        assert!(block.get_record(1).is_some());
    }
}
