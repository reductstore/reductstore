// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod entry_loader;

use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{
    spawn_read_task, spawn_write_task, BlockManager, BlockRef, ManageBlock, RecordTx,
    BLOCK_INDEX_FILE,
};
use crate::storage::bucket::RecordReader;
use crate::storage::entry::entry_loader::EntryLoader;
use crate::storage::proto::record::Label;
use crate::storage::proto::{record, ts_to_us, us_to_ts, Record};
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::{build_query, spawn_query_task, QueryRx};
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::EntryInfo;
use reduct_base::{internal_server_error, too_early, Labels};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

struct QueryHandle {
    rx: QueryRx,
    handle: JoinHandle<()>,
}

/// Entry is a time series in a bucket.
pub(crate) struct Entry {
    name: String,
    settings: EntrySettings,
    block_manager: Arc<RwLock<BlockManager>>,
    queries: HashMap<u64, QueryHandle>,
}

#[derive(PartialEq)]
enum RecordType {
    Latest,
    Belated,
    BelatedFirst,
}

/// EntryOptions is the options for creating a new entry.
#[derive(PartialEq, Debug, Clone)]
pub struct EntrySettings {
    pub max_block_size: u64,
    pub max_block_records: u64,
}

impl Entry {
    pub fn new(name: &str, path: PathBuf, settings: EntrySettings) -> Result<Self, ReductError> {
        fs::create_dir_all(path.join(name))?;

        Ok(Self {
            name: name.to_string(),
            settings,
            block_manager: Arc::new(RwLock::new(BlockManager::new(
                path.join(name),
                BlockIndex::new(path.join(name).join(BLOCK_INDEX_FILE)),
            ))),
            queries: HashMap::new(),
        })
    }

    pub(crate) async fn restore(
        path: PathBuf,
        options: EntrySettings,
    ) -> Result<Entry, ReductError> {
        EntryLoader::restore_entry(path, options).await
    }

    /// Starts a new record write.
    ///
    /// # Arguments
    ///
    /// * `time` - The timestamp of the record.
    /// * `content_size` - The size of the record content.
    /// * `content_type` - The content type of the record.
    /// * `labels` - The labels of the record.
    ///
    /// # Returns
    ///
    /// * `Sender<Result<Bytes, ReductError>>` - The sender to send the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub(crate) async fn begin_write(
        &self,
        time: u64,
        content_size: usize,
        content_type: String,
        labels: Labels,
    ) -> Result<RecordTx, ReductError> {
        // When we write, the likely case is that we are writing the latest record
        // in the entry. In this case, we can just append to the latest block.
        let mut bm = self.block_manager.write().await;

        let mut block_ref = if bm.index().tree().is_empty() {
            bm.start(time, self.settings.max_block_size).await?
        } else {
            let block_id = *bm.index().tree().last().unwrap();
            bm.load(block_id).await?
        };

        let record_type = {
            let is_belated = {
                let block = block_ref.write().await;
                block.record_count() > 0 && block.latest_record_time() >= time
            };
            if is_belated {
                debug!("Timestamp {} is belated. Looking for a block", time);
                // The timestamp is belated. We need to find the proper block to write to.
                let index_tree = bm.index().tree();
                if *index_tree.first().unwrap() > time {
                    // The timestamp is the earliest. We need to create a new block.
                    debug!("Timestamp {} is the earliest. Creating a new block", time);
                    block_ref = bm.start(time, self.settings.max_block_size).await?;
                    RecordType::BelatedFirst
                } else {
                    block_ref = bm.find_block(time).await?;
                    let record = block_ref.read().await.get_record(time).map(|r| r.clone());
                    // check if the record already exists
                    if let Some(mut record) = record {
                        // We overwrite the record if it is errored and the size is the same.
                        return if record.state != record::State::Errored as i32
                            || record.end - record.begin != content_size as u64
                        {
                            Err(ReductError::conflict(&format!(
                                "A record with timestamp {} already exists",
                                time
                            )))
                        } else {
                            {
                                let mut block = block_ref.write().await;
                                record.labels = labels
                                    .into_iter()
                                    .map(|(name, value)| record::Label { name, value })
                                    .collect();
                                record.state = record::State::Started as i32;
                                record.content_type = content_type;
                                block.insert_or_update_record(record);
                            }

                            drop(bm); // drop the lock to avoid deadlock
                            let tx =
                                spawn_write_task(Arc::clone(&self.block_manager), block_ref, time)
                                    .await?;
                            Ok(tx)
                        };
                    }
                    RecordType::Belated
                }
            } else {
                // The timestamp is the latest. We can just append to the latest block.
                RecordType::Latest
            }
        };

        let mut block_ref = {
            let block = block_ref.read().await;
            // Check if the block has enough space for the record.
            let has_no_space = block.size() + content_size as u64 > self.settings.max_block_size;
            let has_too_many_records = block.record_count() + 1 > self.settings.max_block_records;

            drop(block);
            if record_type == RecordType::Latest && (has_no_space || has_too_many_records) {
                // We need to create a new block.
                debug!("Creating a new block");
                bm.finish(block_ref.clone()).await?;
                bm.start(time, self.settings.max_block_size).await?
            } else {
                // We can just append to the latest block.
                block_ref.clone()
            }
        };

        drop(bm);

        self.prepare_block_for_writing(&mut block_ref, time, content_size, content_type, labels)
            .await?;

        let tx = spawn_write_task(Arc::clone(&self.block_manager), block_ref, time).await?;
        Ok(tx)
    }

    async fn prepare_block_for_writing(
        &self,
        block: &mut BlockRef,
        time: u64,
        content_size: usize,
        content_type: String,
        labels: Labels,
    ) -> Result<(), ReductError> {
        let mut block = block.write().await;
        let record = Record {
            timestamp: Some(us_to_ts(&time)),
            begin: block.size(),
            end: block.size() + content_size as u64,
            content_type,
            state: record::State::Started as i32,
            labels: labels
                .into_iter()
                .map(|(name, value)| record::Label { name, value })
                .collect(),
        };

        block.insert_or_update_record(record);
        Ok(())
    }

    /// Starts a new record read.
    ///
    /// # Arguments
    ///
    /// * `time` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader to read the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub(crate) async fn begin_read(&self, time: u64) -> Result<RecordReader, ReductError> {
        debug!("Reading record for ts={}", time);

        let (block_ref, record) = {
            let bm = self.block_manager.read().await;
            let block_ref = bm.find_block(time).await?;
            let block = block_ref.read().await;
            let record = block
                .get_record(time)
                .ok_or_else(|| {
                    ReductError::not_found(&format!("No record with timestamp {}", time))
                })?
                .clone();
            (block_ref.clone(), record)
        };

        if record.state == record::State::Started as i32 {
            return Err(too_early!(
                "Record with timestamp {} is still being written",
                time
            ));
        }

        if record.state == record::State::Errored as i32 {
            return Err(internal_server_error!(
                "Record with timestamp {} is broken",
                time
            ));
        }

        let rx = spawn_read_task(self.block_manager.clone(), block_ref, time).await?;
        Ok(RecordReader::new(rx, record.clone(), true))
    }

    pub async fn update_labels(
        &self,
        time: u64,
        mut update: Labels,
        remove: HashSet<String>,
    ) -> Result<Vec<Label>, ReductError> {
        debug!("Updating labels for ts={}", time);

        let mut bm = self.block_manager.write().await;
        let block_ref = bm.find_block(time).await?;
        let record = {
            let block = block_ref.write().await;
            let mut record = block
                .get_record(time)
                .ok_or_else(|| {
                    ReductError::not_found(&format!("No record with timestamp {}", time))
                })?
                .clone();

            let mut new_labels = Vec::new();
            for label in &record.labels {
                // remove labels
                if remove.contains(label.name.as_str()) {
                    continue;
                }

                // update existing labels or add new labels
                match update.remove(label.name.as_str()) {
                    Some(value) => {
                        new_labels.push(Label {
                            name: label.name.clone(),
                            value,
                        });
                    }
                    None => {
                        new_labels.push(label.clone());
                    }
                }
            }

            // add new labels
            for (name, value) in update {
                new_labels.push(Label { name, value });
            }

            record.labels = new_labels;
            record
        };

        block_ref
            .write()
            .await
            .insert_or_update_record(record.clone());
        bm.update_record(block_ref, record.clone()).await?;
        Ok(record.labels)
    }

    /// Query records for a time range.
    ///
    /// # Arguments
    ///
    /// * `start` - The start time of the query.
    /// * `end` - The end time of the query. Ignored if `continuous` is true.
    /// * `options` - The query options.
    ///
    /// # Returns
    ///
    /// * `u64` - The query ID.
    /// * `HTTPError` - The error if any.
    pub fn query(
        &mut self,
        start: u64,
        end: u64,
        options: QueryOptions,
    ) -> Result<u64, ReductError> {
        static QUERY_ID: AtomicU64 = AtomicU64::new(1); // start with 1 because 0 may confuse with false

        let id = QUERY_ID.fetch_add(1, Ordering::SeqCst);
        self.remove_expired_query();

        let query = build_query(start, end, options)?;
        let block_manager = Arc::clone(&self.block_manager);

        let (rx, handle) = spawn_query_task(query, block_manager);
        self.queries.insert(id, QueryHandle { rx, handle });

        Ok(id)
    }

    /// Returns the next record for a query.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The query ID.
    ///
    /// # Returns
    ///
    /// * `(RecordReader, bool)` - The record reader to read the record content in chunks and a boolean indicating if the query is done.
    /// * `HTTPError` - The error if any.
    pub async fn get_query_receiver(&mut self, query_id: u64) -> Result<&mut QueryRx, ReductError> {
        self.remove_expired_query();
        let query = self
            .queries
            .get_mut(&query_id)
            .ok_or_else(||
                ReductError::not_found(
                    &format!("Query {} not found and it might have expired. Check TTL in your query request. Default value {} sec.", query_id, QueryOptions::default().ttl.as_secs())))?;
        Ok(&mut query.rx)
    }

    /// Returns stats about the entry.
    pub async fn info(&self) -> Result<EntryInfo, ReductError> {
        let bm = self.block_manager.read().await;
        let index_tree = bm.index().tree();
        let (oldest_record, latest_record) = if index_tree.is_empty() {
            (0, 0)
        } else {
            let latest_block_id = index_tree.last().unwrap();
            let latest_record = match bm.index().get_block(*latest_block_id) {
                Some(block) => ts_to_us(&block.latest_record_time.as_ref().unwrap()),
                None => 0,
            };
            (*index_tree.first().unwrap(), latest_record)
        };

        Ok(EntryInfo {
            name: self.name.clone(),
            size: bm.index().size(),
            record_count: bm.index().record_count(),
            block_count: index_tree.len() as u64,
            oldest_record,
            latest_record,
        })
    }

    /// Try to remove the oldest block.
    ///
    /// # Returns
    ///
    /// HTTTPError - The error if any.
    pub async fn try_remove_oldest_block(&mut self) -> Result<(), ReductError> {
        let mut bm = self.block_manager.write().await;
        let index_tree = bm.index().tree();
        if index_tree.is_empty() {
            return Err(ReductError::internal_server_error("No block to remove"));
        }

        let oldest_block_id = *index_tree.first().unwrap();
        bm.remove(oldest_block_id).await?;

        Ok(())
    }

    pub async fn sync_fs(&self) -> Result<(), ReductError> {
        let mut bm = self.block_manager.write().await;
        bm.save_cache_on_disk().await
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn settings(&self) -> &EntrySettings {
        &self.settings
    }

    pub fn set_settings(&mut self, settings: EntrySettings) {
        self.settings = settings;
    }

    fn remove_expired_query(&mut self) {
        self.queries
            .retain(|_, query| !query.handle.is_finished() || !query.rx.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rstest::{fixture, rstest};
    use std::time::Duration;
    use tempfile;
    use tokio::time::sleep;

    mod restore {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
            let mut entry = entry(entry_settings.clone(), path.clone());
            write_stub_record(&mut entry, 1).await.unwrap();
            write_stub_record(&mut entry, 2000010).await.unwrap();

            let mut bm = entry.block_manager.write().await;
            let records = bm
                .load(1)
                .await
                .unwrap()
                .read()
                .await
                .record_index()
                .clone();
            assert_eq!(records.len(), 2);
            assert_eq!(
                *records.get(&1).unwrap(),
                Record {
                    timestamp: Some(us_to_ts(&1)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            assert_eq!(
                *records.get(&2000010).unwrap(),
                Record {
                    timestamp: Some(us_to_ts(&2000010)),
                    begin: 10,
                    end: 20,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            bm.save_cache_on_disk().await.unwrap();
            let entry = Entry::restore(path.join(entry.name), entry_settings)
                .await
                .unwrap();

            let info = entry.info().await.unwrap();
            assert_eq!(info.name, "entry");
            assert_eq!(info.record_count, 2);
            assert_eq!(info.size, 88);
        }
    }

    mod begin_write {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_new_block_size(path: PathBuf) {
            let mut entry = entry(
                EntrySettings {
                    max_block_size: 10,
                    max_block_records: 10000,
                },
                path,
            );

            write_stub_record(&mut entry, 1).await.unwrap();
            write_stub_record(&mut entry, 2000010).await.unwrap();

            let bm = entry.block_manager.read().await;

            assert_eq!(
                bm.load(1)
                    .await
                    .unwrap()
                    .read()
                    .await
                    .get_record(1)
                    .unwrap()
                    .clone(),
                Record {
                    timestamp: Some(us_to_ts(&1)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            assert_eq!(
                bm.load(2000010)
                    .await
                    .unwrap()
                    .read()
                    .await
                    .get_record(2000010)
                    .unwrap()
                    .clone(),
                Record {
                    timestamp: Some(us_to_ts(&2000010)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_new_block_records(path: PathBuf) {
            let mut entry = entry(
                EntrySettings {
                    max_block_size: 10000,
                    max_block_records: 1,
                },
                path,
            );

            write_stub_record(&mut entry, 1).await.unwrap();
            write_stub_record(&mut entry, 2).await.unwrap();
            write_stub_record(&mut entry, 2000010).await.unwrap();

            let bm = entry.block_manager.read().await;
            let records = bm
                .load(1)
                .await
                .unwrap()
                .read()
                .await
                .record_index()
                .clone();
            assert_eq!(
                records.get(&1).unwrap().clone(),
                Record {
                    timestamp: Some(us_to_ts(&1)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            let records = bm
                .load(2000010)
                .await
                .unwrap()
                .read()
                .await
                .record_index()
                .clone();
            assert_eq!(
                records.get(&2000010).unwrap().clone(),
                Record {
                    timestamp: Some(us_to_ts(&2000010)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_belated_record(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();

            let bm = entry.block_manager.read().await;
            let records = bm
                .load(1000000)
                .await
                .unwrap()
                .read()
                .await
                .record_index()
                .clone();
            assert_eq!(records.len(), 3);
            assert_eq!(
                records.get(&1000000).unwrap().timestamp,
                Some(us_to_ts(&1000000))
            );
            assert_eq!(
                records.get(&2000000).unwrap().timestamp,
                Some(us_to_ts(&2000000))
            );
            assert_eq!(
                records.get(&3000000).unwrap().timestamp,
                Some(us_to_ts(&3000000))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_belated_first(mut entry: Entry) {
            write_stub_record(&mut entry, 3000000).await.unwrap();
            write_stub_record(&mut entry, 1000000).await.unwrap();

            let bm = entry.block_manager.read().await;
            let records = bm
                .load(1000000)
                .await
                .unwrap()
                .read()
                .await
                .record_index()
                .clone();
            assert_eq!(records.len(), 1);
            assert_eq!(
                records.get(&1000000).unwrap().timestamp,
                Some(us_to_ts(&1000000))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_existing_record(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();
            let err = write_stub_record(&mut entry, 1000000).await;
            assert_eq!(
                err.err(),
                Some(ReductError::conflict(
                    "A record with timestamp 1000000 already exists"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_existing_record_belated(mut entry: Entry) {
            write_stub_record(&mut entry, 2000000).await.unwrap();
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let err = write_stub_record(&mut entry, 1000000).await;
            assert_eq!(
                err.err(),
                Some(ReductError::conflict(
                    "A record with timestamp 1000000 already exists"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_override_errored(entry: Entry) {
            let sender = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();

            sender.send(Ok(None)).await.unwrap();
            sender.closed().await;

            let sender = entry
                .begin_write(
                    1000000,
                    10,
                    "text/html".to_string(),
                    Labels::from_iter(vec![("a".to_string(), "b".to_string())]),
                )
                .await
                .unwrap();
            sender
                .send(Ok(Some(Bytes::from(vec![0; 10]))))
                .await
                .unwrap();

            let record = entry
                .block_manager
                .write()
                .await
                .load(1000000)
                .await
                .unwrap()
                .read()
                .await
                .get_record(1000000)
                .unwrap()
                .clone();
            assert_eq!(record.content_type, "text/html");
            assert_eq!(record.labels.len(), 1);
            assert_eq!(record.labels[0].name, "a");
            assert_eq!(record.labels[0].value, "b");
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_not_override_if_different_size(entry: Entry) {
            let sender = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();

            sender.send(Ok(None)).await.unwrap();
            sender.closed().await;

            let err = entry
                .begin_write(
                    1000000,
                    5,
                    "text/html".to_string(),
                    Labels::from_iter(vec![("a".to_string(), "b".to_string())]),
                )
                .await
                .err();
            assert_eq!(
                err,
                Some(ReductError::conflict(
                    "A record with timestamp 1000000 already exists"
                ))
            );
        }
    }

    // Test begin_read
    mod begin_read {
        use super::*;
        use crate::storage::storage::DEFAULT_MAX_READ_CHUNK;

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_empty(entry: Entry) {
            let writer = entry.begin_read(1000).await;
            assert_eq!(
                writer.err(),
                Some(ReductError::not_found("No record with timestamp 1000"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_early(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let writer = entry.begin_read(1000).await;
            assert_eq!(
                writer.err(),
                Some(ReductError::not_found("No record with timestamp 1000"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_late(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let reader = entry.begin_read(2000000).await;
            assert_eq!(
                reader.err(),
                Some(ReductError::not_found("No record with timestamp 2000000"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_still_written(entry: Entry) {
            let sender = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();
            sender
                .send(Ok(Some(Bytes::from(vec![0; 5]))))
                .await
                .unwrap();

            let reader = entry.begin_read(1000000).await;
            assert_eq!(
                reader.err(),
                Some(ReductError::too_early(
                    "Record with timestamp 1000000 is still being written"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_not_found(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();

            let reader = entry.begin_read(2000000).await;
            assert_eq!(
                reader.err(),
                Some(ReductError::not_found("No record with timestamp 2000000"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_ok1(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let mut reader = entry.begin_read(1000000).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_ok2(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 1010000).await.unwrap();

            let mut reader = entry.begin_read(1010000).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_read_ok_in_chunks(mut entry: Entry) {
            let mut data = vec![0; DEFAULT_MAX_READ_CHUNK + 1];
            data[0] = 1;
            data[DEFAULT_MAX_READ_CHUNK] = 2;

            write_record(&mut entry, 1000000, data.clone())
                .await
                .unwrap();

            let mut reader = entry.begin_read(1000000).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap().unwrap().to_vec(),
                data[0..DEFAULT_MAX_READ_CHUNK]
            );
            assert_eq!(
                reader.rx().recv().await.unwrap().unwrap().to_vec(),
                data[DEFAULT_MAX_READ_CHUNK..]
            );
            assert_eq!(reader.rx().recv().await, None);
        }

        #[rstest]
        #[tokio::test]
        async fn test_search(path: PathBuf) {
            let mut entry = entry(
                EntrySettings {
                    max_block_size: 10000,
                    max_block_records: 5,
                },
                path,
            );

            let step = 100000;
            for i in 0..100 {
                write_stub_record(&mut entry, i * step).await.unwrap();
            }

            let reader = entry.begin_read(30 * step).await.unwrap();
            assert_eq!(reader.timestamp(), 3000000);
        }
    }

    mod query {
        use super::*;
        use reduct_base::{no_content, not_found};

        #[rstest]
        #[tokio::test]
        async fn test_historical_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();

            let id = entry.query(0, 4000000, QueryOptions::default()).unwrap();
            assert!(id >= 1);

            let rx = entry.get_query_receiver(id).await.unwrap();

            {
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 1000000);
            }
            {
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 2000000);
            }
            {
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 3000000);
            }

            assert_eq!(
                rx.recv().await.unwrap().err(),
                Some(no_content!("No content"))
            );
            assert_eq!(
                entry.get_query_receiver(id).await.err(),
                Some(not_found!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", id))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_continuous_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();

            let id = entry
                .query(
                    0,
                    4000000,
                    QueryOptions {
                        ttl: Duration::from_millis(500),
                        continuous: true,
                        ..QueryOptions::default()
                    },
                )
                .unwrap();

            {
                let rx = entry.get_query_receiver(id).await.unwrap();
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 1000000);
                assert_eq!(
                    rx.recv().await.unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            write_stub_record(&mut entry, 2000000).await.unwrap();
            {
                let rx = entry.get_query_receiver(id).await.unwrap();
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 2000000);
            }

            sleep(Duration::from_millis(700)).await;
            {
                let rx = entry.get_query_receiver(id).await.unwrap();
                assert_eq!(
                    rx.recv().await.unwrap().err(),
                    Some(no_content!("No content"))
                );
            }
            assert_eq!(
                entry.get_query_receiver(id).await.err(),
                Some(not_found!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", id))
            );
        }
    }

    mod update_labels {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_update_labels(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let mut new_labels = entry
                .update_labels(
                    1000000,
                    Labels::from_iter(vec![
                        ("a".to_string(), "x".to_string()),
                        ("e".to_string(), "f".to_string()),
                    ]),
                    HashSet::new(),
                )
                .await
                .unwrap();

            let block_ref = entry
                .block_manager
                .write()
                .await
                .load(1000000)
                .await
                .unwrap();
            let mut record = block_ref.read().await.get_record(1000000).unwrap().clone();
            record.labels.sort_by(|a, b| a.name.cmp(&b.name));
            new_labels.sort_by(|a, b| a.name.cmp(&b.name));

            assert_eq!(record.labels.len(), 3);
            assert_eq!(record.labels[0].name, "a");
            assert_eq!(record.labels[0].value, "x");
            assert_eq!(record.labels[1].name, "c");
            assert_eq!(record.labels[1].value, "d");
            assert_eq!(record.labels[2].name, "e");
            assert_eq!(record.labels[2].value, "f");
            assert_eq!(new_labels, record.labels, "should return new labels");
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_labels_remove(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            entry
                .update_labels(
                    1000000,
                    Labels::new(),
                    HashSet::from_iter(vec!["a".to_string()]),
                )
                .await
                .unwrap();

            let block = entry
                .block_manager
                .write()
                .await
                .load(1000000)
                .await
                .unwrap();
            let record = block.read().await.get_record(1000000).unwrap().clone();
            assert_eq!(record.labels.len(), 1);
            assert_eq!(record.labels[0].name, "c");
            assert_eq!(record.labels[0].value, "d");
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_nothing(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            entry
                .update_labels(1000000, Labels::new(), HashSet::new())
                .await
                .unwrap();

            let block = entry
                .block_manager
                .write()
                .await
                .load(1000000)
                .await
                .unwrap();
            let record = block.read().await.get_record(1000000).unwrap().clone();
            assert_eq!(record.labels.len(), 2);
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_labels_not_found(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let err = entry
                .update_labels(
                    1000000,
                    Labels::new(),
                    HashSet::from_iter(vec!["x".to_string()]),
                )
                .await;

            assert!(err.is_ok(), "Ignore removing labels that are not found")
        }

        async fn write_stub_record(mut entry: &mut Entry, time: u64) -> Result<(), ReductError> {
            write_record_with_labels(
                &mut entry,
                time,
                vec![],
                Labels::from_iter(vec![
                    ("a".to_string(), "b".to_string()),
                    ("c".to_string(), "d".to_string()),
                ]),
            )
            .await
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_info(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 10000,
            },
            path,
        );

        write_stub_record(&mut entry, 1000000).await.unwrap();
        write_stub_record(&mut entry, 2000000).await.unwrap();
        write_stub_record(&mut entry, 3000000).await.unwrap();

        let info = entry.info().await.unwrap();
        assert_eq!(info.name, "entry");
        assert_eq!(info.size, 88);
        assert_eq!(info.record_count, 3);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1000000);
        assert_eq!(info.latest_record, 3000000);
    }

    mod try_remove_oldest_block {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_empty_entry(mut entry: Entry) {
            assert_eq!(
                entry.try_remove_oldest_block().await,
                Err(ReductError::internal_server_error("No block to remove"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_entry_which_has_reader(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            let _rx = entry.begin_read(1000000).await.unwrap();

            assert_eq!(
                entry.try_remove_oldest_block().await,
                Err(ReductError::internal_server_error(
                    "Cannot remove block 1000000 because it is still in use"
                ))
            );
            let info = entry.info().await.unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 28);
        }

        #[rstest]
        #[tokio::test]
        async fn test_entry_which_has_writer(mut entry: Entry) {
            let sender = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();
            sender
                .send(Ok(Some(Bytes::from_static(b"456789"))))
                .await
                .unwrap();

            assert_eq!(
                entry.try_remove_oldest_block().await,
                Err(ReductError::internal_server_error(
                    "Cannot remove block 1000000 because it is still in use"
                ))
            );
            let info = entry.info().await.unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 28);
        }

        #[rstest]
        #[tokio::test]
        async fn test_size_counting(path: PathBuf) {
            let mut entry = Entry::new(
                "entry",
                path.clone(),
                EntrySettings {
                    max_block_size: 100000,
                    max_block_records: 2,
                },
            )
            .unwrap();

            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();
            write_stub_record(&mut entry, 4000000).await.unwrap();

            assert_eq!(entry.info().await.unwrap().block_count, 2);
            assert_eq!(entry.info().await.unwrap().record_count, 4);
            assert_eq!(entry.info().await.unwrap().size, 138);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().await.unwrap().block_count, 1);
            assert_eq!(entry.info().await.unwrap().record_count, 2);
            assert_eq!(entry.info().await.unwrap().size, 58);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().await.unwrap().block_count, 0);
            assert_eq!(entry.info().await.unwrap().record_count, 0);
            assert_eq!(entry.info().await.unwrap().size, 0);
        }
    }

    #[fixture]
    pub(super) fn entry_settings() -> EntrySettings {
        EntrySettings {
            max_block_size: 10000,
            max_block_records: 10000,
        }
    }

    #[fixture]
    pub(super) fn entry(entry_settings: EntrySettings, path: PathBuf) -> Entry {
        Entry::new("entry", path.clone(), entry_settings).unwrap()
    }

    #[fixture]
    pub(super) fn path() -> PathBuf {
        tempfile::tempdir().unwrap().into_path()
    }

    async fn write_record(entry: &mut Entry, time: u64, data: Vec<u8>) -> Result<(), ReductError> {
        let sender = entry
            .begin_write(time, data.len(), "text/plain".to_string(), Labels::new())
            .await?;
        let x = sender.send(Ok(Some(Bytes::from(data)))).await;
        sender.closed().await;
        drop(sender);
        match x {
            Ok(_) => Ok(()),
            Err(_) => Err(ReductError::internal_server_error("Error sending data")),
        }
    }

    async fn write_record_with_labels(
        entry: &mut Entry,
        time: u64,
        data: Vec<u8>,
        labels: Labels,
    ) -> Result<(), ReductError> {
        let sender = entry
            .begin_write(time, data.len(), "text/plain".to_string(), labels)
            .await?;
        let x = sender.send(Ok(Some(Bytes::from(data)))).await;
        sender.closed().await;
        match x {
            Ok(_) => Ok(()),
            Err(_) => Err(ReductError::internal_server_error("Error sending data")),
        }
    }

    pub(super) async fn write_stub_record(entry: &mut Entry, time: u64) -> Result<(), ReductError> {
        write_record(entry, time, b"0123456789".to_vec()).await
    }
}
