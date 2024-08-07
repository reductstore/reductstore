// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::{
    find_first_block, spawn_read_task, spawn_write_task, BlockManager, ManageBlock, RecordTx,
    DATA_FILE_EXT, DESCRIPTOR_FILE_EXT,
};
use crate::storage::bucket::RecordReader;
use crate::storage::proto::record::Label;
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block, MinimalBlock, Record};
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::build_query;
use bytesize::ByteSize;
use log::{debug, error, warn};
use prost::bytes::Bytes;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::EntryInfo;
use reduct_base::Labels;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Entry is a time series in a bucket.
pub struct Entry {
    name: String,
    settings: EntrySettings,
    block_index: BTreeSet<u64>,
    block_manager: Arc<RwLock<BlockManager>>,
    record_count: u64,
    size: u64,
    queries: HashMap<u64, Box<dyn Query + Send + Sync>>,
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
    pub(crate) fn new(
        name: &str,
        path: PathBuf,
        settings: EntrySettings,
    ) -> Result<Self, ReductError> {
        fs::create_dir_all(path.join(name))?;
        Ok(Self {
            name: name.to_string(),
            settings,
            block_index: BTreeSet::new(),
            block_manager: Arc::new(RwLock::new(BlockManager::new(path.join(name)))),
            record_count: 0,
            size: 0,
            queries: HashMap::new(),
        })
    }

    pub(crate) fn restore(path: PathBuf, options: EntrySettings) -> Result<Self, ReductError> {
        let mut record_count = 0;
        let mut size = 0;
        let mut block_index = BTreeSet::new();
        let start_time = std::time::Instant::now();

        for filein in fs::read_dir(path.clone())? {
            let file = filein?;
            let path = file.path();
            if path.is_dir() {
                continue;
            }
            let name = path.file_name().unwrap().to_str().unwrap();
            if !name.ends_with(DESCRIPTOR_FILE_EXT) {
                continue;
            }

            macro_rules! remove_bad_block {
                ($err:expr) => {{
                    error!("Failed to decode block {:?}: {}", path, $err);
                    warn!("Removing meta block {:?}", path);
                    let mut data_path = path.clone();
                    fs::remove_file(path)?;

                    data_path.set_extension(DATA_FILE_EXT[1..].to_string());
                    warn!("Removing data block {:?}", data_path);
                    fs::remove_file(data_path)?;
                    continue;
                }};
            }

            let buf = fs::read(path.clone())?;
            let mut block = match MinimalBlock::decode(Bytes::from(buf)) {
                Ok(block) => block,
                Err(err) => {
                    remove_bad_block!(err);
                }
            };

            // Migration for old blocks without fields to speed up the restore process
            // todo: remove this data_check in the future rel 1.12
            if block.record_count == 0 {
                debug!("Record count is 0. Migrate the block");
                let mut full_block = match Block::decode(Bytes::from(fs::read(path.clone())?)) {
                    Ok(block) => block,
                    Err(err) => {
                        remove_bad_block!(err);
                    }
                };

                full_block.record_count = full_block.records.len() as u64;
                full_block.metadata_size = full_block.encoded_len() as u64;

                block.record_count = full_block.record_count;
                block.metadata_size = full_block.metadata_size;

                let mut file = fs::File::create(path.clone())?;
                file.write_all(&full_block.encode_to_vec())?;
            }

            let id = if let Some(begin_time) = block.begin_time {
                ts_to_us(&begin_time)
            } else {
                remove_bad_block!("begin time mismatch");
            };

            if block.invalid {
                remove_bad_block!("block is invalid");
            }

            record_count += block.record_count as u64;
            size += block.size + block.metadata_size;
            block_index.insert(id);
        }

        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        debug!(
            "Restored entry `{}` in {}ms: size={}, records={}",
            name,
            start_time.elapsed().as_millis(),
            ByteSize::b(size),
            record_count
        );
        Ok(Self {
            name,
            settings: options,
            block_index,
            block_manager: Arc::new(RwLock::new(BlockManager::new(path))),
            record_count,
            size,
            queries: HashMap::new(),
        })
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
        &mut self,
        time: u64,
        content_size: usize,
        content_type: String,
        labels: Labels,
    ) -> Result<RecordTx, ReductError> {
        // When we write, the likely case is that we are writing the latest record
        // in the entry. In this case, we can just append to the latest block.
        let block = if self.block_index.is_empty() {
            self.start_new_block(time).await?
        } else {
            let bm = self.block_manager.write().await;
            bm.load(*self.block_index.last().unwrap()).await?
        };

        let (block, record_type) = if block.latest_record_time.is_some()
            && ts_to_us(block.latest_record_time.as_ref().unwrap()) >= time
        {
            debug!("Timestamp {} is belated. Looking for a block", time);
            // The timestamp is belated. We need to find the proper block to write to.

            if *self.block_index.first().unwrap() > time {
                // The timestamp is the earliest. We need to create a new block.
                debug!("Timestamp {} is the earliest. Creating a new block", time);
                (self.start_new_block(time).await?, RecordType::BelatedFirst)
            } else {
                let block_id = find_first_block(&self.block_index, &time);
                let mut block = async {
                    let bm = self.block_manager.write().await;
                    bm.load(block_id).await
                }
                .await?;

                // check if the record already exists
                let proto_time = Some(us_to_ts(&time));
                if let Some(record) = block
                    .records
                    .iter_mut()
                    .filter(|record| record.timestamp == proto_time)
                    .last()
                {
                    // We overwrite the record if it is errored and the size is the same.
                    return if record.state != record::State::Errored as i32
                        || record.end - record.begin != content_size as u64
                    {
                        Err(ReductError::conflict(&format!(
                            "A record with timestamp {} already exists",
                            time
                        )))
                    } else {
                        record.labels = labels
                            .into_iter()
                            .map(|(name, value)| record::Label { name, value })
                            .collect();
                        record.state = record::State::Started as i32;
                        record.content_type = content_type;

                        let record_index = block
                            .records
                            .iter()
                            .position(|r| r.timestamp == proto_time)
                            .unwrap();
                        let tx =
                            spawn_write_task(Arc::clone(&self.block_manager), block, record_index)
                                .await?;
                        Ok(tx)
                    };
                }
                (block, RecordType::Belated)
            }
        } else {
            // The timestamp is the latest. We can just append to the latest block.
            (block, RecordType::Latest)
        };

        let block = self
            .prepare_block_for_writing(time, content_size, content_type, labels, block, record_type)
            .await?;
        let record_index = block.records.len() - 1;

        let tx = spawn_write_task(Arc::clone(&self.block_manager), block, record_index).await?;
        Ok(tx)
    }

    async fn prepare_block_for_writing(
        &mut self,
        time: u64,
        content_size: usize,
        content_type: String,
        labels: Labels,
        block: Block,
        record_type: RecordType,
    ) -> Result<Block, ReductError> {
        let has_no_space = block.size + content_size as u64 > self.settings.max_block_size;
        let has_too_many_records =
            block.records.len() + 1 > self.settings.max_block_records as usize;

        let mut block = if record_type == RecordType::Latest
            && (has_no_space || has_too_many_records || block.invalid)
        {
            // We need to create a new block.
            debug!("Creating a new block");
            self.block_manager.write().await.finish(&block).await?;
            self.start_new_block(time).await?
        } else {
            // We can just append to the latest block.
            block
        };

        let record = Record {
            timestamp: Some(us_to_ts(&time)),
            begin: block.size,
            end: block.size + content_size as u64,
            content_type,
            state: record::State::Started as i32,
            labels: labels
                .into_iter()
                .map(|(name, value)| record::Label { name, value })
                .collect(),
        };

        // count the record and usage size with protobuf overhead
        self.record_count += 1;
        self.size += content_size as u64;

        block.size += content_size as u64;
        block.records.push(record);
        block.record_count = block.records.len() as u64;

        // recalculate the metadata size
        if block.records.len() > 1 {
            self.size -= block.metadata_size;
        }
        block.metadata_size = block.encoded_len() as u64;
        self.size += block.metadata_size;

        if record_type != RecordType::Belated {
            block.latest_record_time = Some(us_to_ts(&time));
        }
        Ok(block)
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

        let (block_id, block, record_index) = self.find_record(&time).await?;
        let mut record = block.records[record_index].clone();

        if record.state == record::State::Started as i32 {
            let block = {
                let bm = self.block_manager.write().await;
                bm.load(block_id).await?
            };

            record = block.records[record_index].clone();
            if record.state == record::State::Started as i32 {
                return Err(ReductError::too_early(&format!(
                    "Record with timestamp {} is still being written",
                    time
                )));
            }
        }

        if record.state == record::State::Errored as i32 {
            return Err(ReductError::internal_server_error(&format!(
                "Record with timestamp {} is broken",
                time
            )));
        }

        let rx = spawn_read_task(Arc::clone(&self.block_manager), &block, record_index).await?;
        Ok(RecordReader::new(rx, record, true))
    }

    async fn find_record(&self, time: &u64) -> Result<(u64, Block, usize), ReductError> {
        let not_found_err = Err(ReductError::not_found(&format!(
            "No record with timestamp {}",
            time
        )));

        if self.block_index.is_empty() {
            return not_found_err;
        }

        if time < self.block_index.first().unwrap() {
            return not_found_err;
        }

        let block_id = find_first_block(&self.block_index, &time);
        let block = {
            let bm = self.block_manager.write().await;
            bm.load(block_id).await?
        };

        let record_index = block
            .records
            .iter()
            .position(|record| ts_to_us(record.timestamp.as_ref().unwrap()) == *time);

        if record_index.is_none() {
            return not_found_err;
        }

        let record_index = record_index.unwrap();
        Ok((block_id, block, record_index))
    }

    pub async fn update_labels(
        &mut self,
        time: u64,
        mut update: Labels,
        remove: HashSet<String>,
    ) -> Result<Vec<Label>, ReductError> {
        debug!("Updating labels for ts={}", time);

        let (_, mut block, record_index) = self.find_record(&time).await?;
        let record = block.records.get_mut(record_index).unwrap();

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

        record.labels = new_labels.clone();
        self.block_manager.write().await.save(block).await?;
        Ok(new_labels)
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
        self.queries.insert(id, build_query(start, end, options)?);

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
    pub async fn next(&mut self, query_id: u64) -> Result<RecordReader, ReductError> {
        self.remove_expired_query();
        let query = self
            .queries
            .get_mut(&query_id)
            .ok_or_else(||
                ReductError::not_found(
                    &format!("Query {} not found and it might have expired. Check TTL in your query request. Default value {} sec.", query_id, QueryOptions::default().ttl.as_secs())))?;
        query
            .next(&self.block_index, Arc::clone(&self.block_manager))
            .await
    }

    /// Returns stats about the entry.
    pub async fn info(&self) -> Result<EntryInfo, ReductError> {
        let (oldest_record, latest_record) = if self.block_index.is_empty() {
            (0, 0)
        } else {
            let latest_block = self
                .block_manager
                .read()
                .await
                .load(*self.block_index.last().unwrap())
                .await?;
            let latest_record = if latest_block.records.is_empty() {
                0
            } else {
                ts_to_us(
                    latest_block
                        .records
                        .iter()
                        .last()
                        .unwrap()
                        .timestamp
                        .as_ref()
                        .unwrap(),
                )
            };
            (*self.block_index.first().unwrap(), latest_record)
        };

        Ok(EntryInfo {
            name: self.name.clone(),
            size: self.size,
            record_count: self.record_count,
            block_count: self.block_index.len() as u64,
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
        if self.block_index.is_empty() {
            return Err(ReductError::internal_server_error("No block to remove"));
        }

        let oldest_block_id = *self.block_index.first().unwrap();
        let block = self
            .block_manager
            .write()
            .await
            .load(oldest_block_id)
            .await?;

        self.block_manager
            .write()
            .await
            .remove(oldest_block_id)
            .await?;
        self.block_index.remove(&oldest_block_id);

        self.size -= block.size + block.metadata_size;
        self.record_count -= block.records.len() as u64;

        Ok(())
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

    async fn start_new_block(&mut self, time: u64) -> Result<Block, ReductError> {
        let block = self
            .block_manager
            .write()
            .await
            .start(time, self.settings.max_block_size)
            .await?;
        self.block_index
            .insert(ts_to_us(&block.begin_time.as_ref().unwrap()));
        Ok::<Block, ReductError>(block)
    }

    fn remove_expired_query(&mut self) {
        self.queries
            .retain(|_, query| matches!(query.state(), QueryState::Running(_)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

            let bm = entry.block_manager.read().await;
            let records = bm.load(1).await.unwrap().records.clone();
            assert_eq!(records.len(), 2);
            assert_eq!(
                records[0],
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
                records[1],
                Record {
                    timestamp: Some(us_to_ts(&2000010)),
                    begin: 10,
                    end: 20,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            let entry = Entry::restore(path.join(entry.name), entry_settings).unwrap();

            assert_eq!(entry.name(), "entry");
            assert_eq!(entry.record_count, 2);
            assert_eq!(entry.size, 84);
        }

        #[rstest]
        #[tokio::test]
        async fn test_restore_bad_block(entry_settings: EntrySettings, path: PathBuf) {
            let mut entry = entry(entry_settings.clone(), path.clone());

            write_stub_record(&mut entry, 1).await.unwrap();

            let meta_path = path.join("entry/1.meta");
            fs::write(meta_path.clone(), b"bad data").unwrap();
            let data_path = path.join("entry/1.blk");
            fs::write(data_path.clone(), b"bad data").unwrap();

            let entry = Entry::restore(path.join(entry.name), entry_settings).unwrap();
            assert_eq!(entry.name(), "entry");
            assert_eq!(entry.record_count, 0);

            assert!(!meta_path.exists(), "should remove meta block");
            assert!(!data_path.exists(), "should remove data block");
        }

        #[rstest]
        #[tokio::test]
        async fn test_migration_v18_v19(entry_settings: EntrySettings, path: PathBuf) {
            let path = path.join("entry");
            fs::create_dir_all(path.clone()).unwrap();
            let mut block_manager = BlockManager::new(path.clone());
            let mut block_v18 = block_manager.start(1, 100).await.unwrap();
            block_v18.records.push(Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            });
            block_v18.records.push(Record {
                timestamp: Some(us_to_ts(&2)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            });
            block_v18.size = 10;
            block_v18.begin_time = Some(us_to_ts(&1));
            block_manager.save(block_v18).await.unwrap();

            // repack the block
            let entry = Entry::restore(path.clone(), entry_settings).unwrap();
            let info = entry.info().await.unwrap();

            assert_eq!(info.size, 65);
            assert_eq!(info.record_count, 2);
            assert_eq!(info.block_count, 1);
            assert_eq!(info.oldest_record, 1);
            assert_eq!(info.latest_record, 2);

            let block_manager = BlockManager::new(path); // reload the block manager
            let block_v19 = block_manager.load(1).await.unwrap();
            assert_eq!(block_v19.record_count, 2);
            assert_eq!(block_v19.size, 10);
            assert_eq!(block_v19.metadata_size, 55);
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
                bm.load(1).await.unwrap().records[0],
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
                bm.load(2000010).await.unwrap().records[0],
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
            assert_eq!(
                bm.load(1).await.unwrap().records[0],
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
                bm.load(2000010).await.unwrap().records[0],
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
            let records = bm.load(1000000).await.unwrap().records.clone();
            assert_eq!(records.len(), 3);
            assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
            assert_eq!(records[1].timestamp, Some(us_to_ts(&3000000)));
            assert_eq!(records[2].timestamp, Some(us_to_ts(&2000000)));
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_write_belated_first(mut entry: Entry) {
            write_stub_record(&mut entry, 3000000).await.unwrap();
            write_stub_record(&mut entry, 1000000).await.unwrap();

            let bm = entry.block_manager.read().await;
            let records = bm.load(1000000).await.unwrap().records.clone();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
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
        async fn test_begin_override_errored(mut entry: Entry) {
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
                .read()
                .await
                .load(1000000)
                .await
                .unwrap()
                .records[0]
                .clone();
            assert_eq!(record.content_type, "text/html");
            assert_eq!(record.labels.len(), 1);
            assert_eq!(record.labels[0].name, "a");
            assert_eq!(record.labels[0].value, "b");
        }

        #[rstest]
        #[tokio::test]
        async fn test_begin_not_override_if_different_size(mut entry: Entry) {
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
        async fn test_begin_read_still_written(mut entry: Entry) {
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

        #[rstest]
        #[tokio::test]
        async fn test_historical_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();

            let id = entry.query(0, 4000000, QueryOptions::default()).unwrap();
            assert!(id >= 1);

            {
                let reader = entry.next(id).await.unwrap();
                assert_eq!(reader.timestamp(), 1000000);
            }
            {
                let reader = entry.next(id).await.unwrap();
                assert_eq!(reader.timestamp(), 2000000);
            }
            {
                let reader = entry.next(id).await.unwrap();
                assert_eq!(reader.timestamp(), 3000000);
            }

            assert_eq!(
                entry.next(id).await.err(),
                Some(ReductError::no_content("No content"))
            );
            assert_eq!(
                entry.next(id).await.err(),
                Some(ReductError::not_found(&format!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", id)))
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
                let reader = entry.next(id).await.unwrap();
                assert_eq!(reader.timestamp(), 1000000);
            }

            assert_eq!(
                entry.next(id).await.err(),
                Some(ReductError::no_content("No content"))
            );

            write_stub_record(&mut entry, 2000000).await.unwrap();
            {
                let reader = entry.next(id).await.unwrap();
                assert_eq!(reader.timestamp(), 2000000);
            }

            sleep(Duration::from_millis(600)).await;
            assert_eq!(
                entry.next(id).await.err(),
                Some(ReductError::not_found(&format!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", id)))
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

            let mut block = entry
                .block_manager
                .read()
                .await
                .load(1000000)
                .await
                .unwrap();
            block.records[0].labels.sort_by(|a, b| a.name.cmp(&b.name));
            new_labels.sort_by(|a, b| a.name.cmp(&b.name));

            assert_eq!(block.records[0].labels.len(), 3);
            assert_eq!(block.records[0].labels[0].name, "a");
            assert_eq!(block.records[0].labels[0].value, "x");
            assert_eq!(block.records[0].labels[1].name, "c");
            assert_eq!(block.records[0].labels[1].value, "d");
            assert_eq!(block.records[0].labels[2].name, "e");
            assert_eq!(block.records[0].labels[2].value, "f");
            assert_eq!(
                new_labels, block.records[0].labels,
                "should return new labels"
            );
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
                .read()
                .await
                .load(1000000)
                .await
                .unwrap();
            assert_eq!(block.records[0].labels.len(), 1);
            assert_eq!(block.records[0].labels[0].name, "c");
            assert_eq!(block.records[0].labels[0].value, "d");
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
                .read()
                .await
                .load(1000000)
                .await
                .unwrap();
            assert_eq!(block.records[0].labels.len(), 2);
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
        assert_eq!(info.size, 112);
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
            assert_eq!(info.size, 38);
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
            assert_eq!(info.size, 38);
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
            assert_eq!(entry.info().await.unwrap().size, 156);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().await.unwrap().block_count, 1);
            assert_eq!(entry.info().await.unwrap().record_count, 2);
            assert_eq!(entry.info().await.unwrap().size, 78);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().await.unwrap().block_count, 0);
            assert_eq!(entry.info().await.unwrap().record_count, 0);
            assert_eq!(entry.info().await.unwrap().size, 0);
        }
    }

    #[fixture]
    fn entry_settings() -> EntrySettings {
        EntrySettings {
            max_block_size: 10000,
            max_block_records: 10000,
        }
    }

    #[fixture]
    fn entry(entry_settings: EntrySettings, path: PathBuf) -> Entry {
        Entry::new("entry", path.clone(), entry_settings).unwrap()
    }

    #[fixture]
    fn path() -> PathBuf {
        tempfile::tempdir().unwrap().into_path()
    }

    async fn write_record(entry: &mut Entry, time: u64, data: Vec<u8>) -> Result<(), ReductError> {
        let sender = entry
            .begin_write(time, data.len(), "text/plain".to_string(), Labels::new())
            .await?;
        let x = sender.send(Ok(Some(Bytes::from(data)))).await;
        sender.closed().await;
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

    async fn write_stub_record(entry: &mut Entry, time: u64) -> Result<(), ReductError> {
        write_record(entry, time, b"0123456789".to_vec()).await
    }
}
