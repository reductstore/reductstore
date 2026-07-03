// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub async fn find_block(&mut self, start: u64) -> Result<BlockRef, ReductError> {
        let id = self.find_block_id(start)?;
        self.load_block(id).await
    }

    fn find_block_id(&self, start: u64) -> Result<u64, ReductError> {
        let active_tree = self.block_index.active_tree();
        let start_block_id = active_tree.range(start..).next();
        if start_block_id.is_some() && start >= *start_block_id.unwrap() {
            Ok(*start_block_id.unwrap())
        } else if let Some(block_id) = active_tree.range(..start).rev().next() {
            Ok(*block_id)
        } else {
            Err(ReductError::not_found(&format!(
                "Record {} not found in entry {}/{}",
                start, self.bucket, self.entry
            )))
        }
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
