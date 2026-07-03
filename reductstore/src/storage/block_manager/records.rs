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
