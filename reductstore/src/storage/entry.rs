// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::{
    find_first_block, BlockManager, ManageBlock, DESCRIPTOR_FILE_EXT,
};
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block, Record};
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::build_query;
use crate::storage::reader::RecordReader;
use crate::storage::writer::RecordWriter;
use log::{debug, error, warn};
use prost::bytes::Bytes;
use prost::Message;
use reduct_base::error::ReductError;

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;

use reduct_base::msg::entry_api::EntryInfo;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub type Labels = HashMap<String, String>;

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
            block_index: BTreeSet::new(),
            block_manager: Arc::new(RwLock::new(BlockManager::new(path.join(name)))),
            record_count: 0,
            size: 0,
            queries: HashMap::new(),
        })
    }

    pub fn restore(path: PathBuf, options: EntrySettings) -> Result<Self, ReductError> {
        let mut record_count = 0;
        let mut size = 0;
        let mut block_index = BTreeSet::new();
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
                    warn!("Removing block {:?}", path);
                    fs::remove_file(path)?;
                    continue;
                }};
            }

            let buf = std::fs::read(path.clone())?;
            let block = match Block::decode(Bytes::from(buf)) {
                Ok(block) => block,
                Err(err) => {
                    remove_bad_block!(err);
                }
            };

            let id = if let Some(begin_time) = block.begin_time {
                ts_to_us(&begin_time)
            } else {
                remove_bad_block!("begin time mismatch");
            };

            record_count += block.records.len() as u64;
            size += block.size;
            block_index.insert(id);
        }

        Ok(Self {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
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
    /// * `RecordWriter` - The record writer to write the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub fn begin_write(
        &mut self,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<Arc<RwLock<RecordWriter>>, ReductError> {
        #[derive(PartialEq)]
        enum RecordType {
            Latest,
            Belated,
            BelatedFirst,
        }

        // When we write, the likely case is that we are writing the latest record
        // in the entry. In this case, we can just append to the latest block.
        let block = if self.block_index.is_empty() {
            self.start_new_block(time)?
        } else {
            let bm = self.block_manager.read().unwrap();
            bm.load(*self.block_index.last().unwrap())?
        };

        let (block, record_type) = if block.latest_record_time.is_some()
            && ts_to_us(block.latest_record_time.as_ref().unwrap()) >= time
        {
            debug!("Timestamp {} is belated. Looking for a block", time);
            // The timestamp is belated. We need to find the proper block to write to.

            if *self.block_index.first().unwrap() > time {
                // The timestamp is the earliest. We need to create a new block.
                debug!("Timestamp {} is the earliest. Creating a new block", time);
                (self.start_new_block(time)?, RecordType::BelatedFirst)
            } else {
                let block_id = find_first_block(&self.block_index, &time);
                let bm = self.block_manager.read().unwrap();
                let block = bm.load(block_id)?;
                // check if the record already exists
                let proto_time = Some(us_to_ts(&time));
                if block
                    .records
                    .iter()
                    .any(|record| record.timestamp == proto_time)
                {
                    return Err(ReductError::conflict(&format!(
                        "A record with timestamp {} already exists",
                        time
                    )));
                }
                (block, RecordType::Belated)
            }
        } else {
            // The timestamp is the latest. We can just append to the latest block.
            (block, RecordType::Latest)
        };

        let has_no_space = block.size + content_size > self.settings.max_block_size;
        let has_too_many_records =
            block.records.len() + 1 >= self.settings.max_block_records as usize;

        let mut block = if record_type == RecordType::Latest
            && (has_no_space || has_too_many_records || block.invalid)
        {
            // We need to create a new block.
            debug!("Creating a new block");
            self.block_manager.write().unwrap().finish(&block)?;
            self.start_new_block(time)?
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

        block.size += content_size as u64;
        block.records.push(record);

        self.record_count += 1;
        self.size += content_size as u64;
        if record_type != RecordType::Belated {
            block.latest_record_time = Some(us_to_ts(&time));
        }

        self.block_manager.write().unwrap().save(block.clone())?;
        BlockManager::begin_write(
            Arc::clone(&self.block_manager),
            &block,
            block.records.len() - 1,
        )
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
    pub(crate) fn begin_read(&self, time: u64) -> Result<Arc<RwLock<RecordReader>>, ReductError> {
        debug!("Reading record for ts={}", time);
        let not_found_err = Err(ReductError::not_found(&format!(
            "No record with timestamp {}",
            time
        )));

        if self.block_index.is_empty() {
            return not_found_err;
        }

        if &time < self.block_index.first().unwrap() {
            return not_found_err;
        }

        let _block_id = self.block_index.range(time..).next();
        let block_id = find_first_block(&self.block_index, &time);

        let block = {
            let bm = self.block_manager.read().unwrap();
            bm.load(block_id)?
        };

        let record_index = block
            .records
            .iter()
            .position(|record| ts_to_us(record.timestamp.as_ref().unwrap()) == time);

        if record_index.is_none() {
            return not_found_err;
        }

        let record = &block.records[record_index.unwrap()];
        if record.state == record::State::Started as i32 {
            return Err(ReductError::too_early(&format!(
                "Record with timestamp {} is still being written",
                time
            )));
        }

        if record.state == record::State::Errored as i32 {
            return Err(ReductError::internal_server_error(&format!(
                "Record with timestamp {} is broken",
                time
            )));
        }

        self.block_manager
            .write()
            .unwrap()
            .begin_read(&block, record_index.unwrap())
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
    pub fn next(
        &mut self,
        query_id: u64,
    ) -> Result<(Arc<RwLock<RecordReader>>, bool), ReductError> {
        self.remove_expired_query();
        let query = self
            .queries
            .get_mut(&query_id)
            .ok_or_else(|| ReductError::not_found(&format!("Query {} not found", query_id)))?;
        query.next(&self.block_index, &mut self.block_manager.write().unwrap())
    }

    /// Returns stats about the entry.
    pub fn info(&self) -> Result<EntryInfo, ReductError> {
        let (oldest_record, latest_record) = if self.block_index.is_empty() {
            (0, 0)
        } else {
            let latest_block = self
                .block_manager
                .read()
                .unwrap()
                .load(*self.block_index.last().unwrap())?;
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
    pub fn try_remove_oldest_block(&mut self) -> Result<(), ReductError> {
        if self.block_index.is_empty() {
            return Err(ReductError::internal_server_error("No block to remove"));
        }

        let oldest_block_id = *self.block_index.first().unwrap();

        let block = self.block_manager.read().unwrap().load(oldest_block_id)?;
        self.size -= block.size;
        self.record_count -= block.records.len() as u64;

        self.block_manager
            .write()
            .unwrap()
            .remove(oldest_block_id)?;
        self.block_index.remove(&oldest_block_id);

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

    fn start_new_block(&mut self, time: u64) -> Result<Block, ReductError> {
        let block = self
            .block_manager
            .write()
            .unwrap()
            .start(time, self.settings.max_block_size)?;
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
    use crate::storage::block_manager::DEFAULT_MAX_READ_CHUNK;
    use crate::storage::writer::Chunk;
    use std::fs::File;

    use rstest::{fixture, rstest};
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile;

    #[rstest]
    #[test]
    fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
        let mut entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&mut entry, 1).unwrap();
        write_stub_record(&mut entry, 2000010).unwrap();

        let bm = entry.block_manager.read().unwrap();
        let records = bm.load(1).unwrap().records.clone();
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
        assert_eq!(entry.size, 20);
    }

    #[rstest]
    #[test]
    fn test_restore_bad_block(entry_settings: EntrySettings, path: PathBuf) {
        let mut entry = entry(entry_settings.clone(), path.clone());

        write_stub_record(&mut entry, 1).unwrap();

        File::options()
            .write(true)
            .open(path.join("entry/1.meta"))
            .unwrap()
            .set_len(0)
            .unwrap();

        let entry = Entry::restore(path.join(entry.name), entry_settings).unwrap();
        assert_eq!(entry.name(), "entry");
        assert_eq!(entry.record_count, 0);
    }

    #[rstest]
    #[test]
    fn test_begin_write_new_block_size(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10,
                max_block_records: 10000,
            },
            path,
        );

        write_stub_record(&mut entry, 1).unwrap();
        write_stub_record(&mut entry, 2000010).unwrap();

        let bm = entry.block_manager.read().unwrap();

        assert_eq!(
            bm.load(1).unwrap().records[0],
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
            bm.load(2000010).unwrap().records[0],
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
    #[test]
    fn test_begin_write_new_block_records(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 1,
            },
            path,
        );

        write_stub_record(&mut entry, 1).unwrap();
        write_stub_record(&mut entry, 2).unwrap();
        write_stub_record(&mut entry, 2000010).unwrap();

        let bm = entry.block_manager.read().unwrap();
        assert_eq!(
            bm.load(1).unwrap().records[0],
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
            bm.load(2000010).unwrap().records[0],
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
    #[test]
    fn test_begin_write_belated_record(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        write_stub_record(&mut entry, 3000000).unwrap();
        write_stub_record(&mut entry, 2000000).unwrap();

        let bm = entry.block_manager.read().unwrap();
        let records = bm.load(1000000).unwrap().records.clone();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
        assert_eq!(records[1].timestamp, Some(us_to_ts(&3000000)));
        assert_eq!(records[2].timestamp, Some(us_to_ts(&2000000)));
    }

    #[rstest]
    #[test]
    fn test_begin_write_belated_first(mut entry: Entry) {
        write_stub_record(&mut entry, 3000000).unwrap();
        write_stub_record(&mut entry, 1000000).unwrap();

        let bm = entry.block_manager.read().unwrap();
        let records = bm.load(1000000).unwrap().records.clone();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
    }

    #[rstest]
    #[test]
    fn test_begin_write_existing_record(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        write_stub_record(&mut entry, 2000000).unwrap();
        let err = write_stub_record(&mut entry, 1000000);
        assert_eq!(
            err.err(),
            Some(ReductError::conflict(
                "A record with timestamp 1000000 already exists"
            ))
        );
    }

    // Test begin_read
    #[rstest]
    #[test]
    fn test_begin_read_empty(entry: Entry) {
        let writer = entry.begin_read(1000);
        assert_eq!(
            writer.err(),
            Some(ReductError::not_found("No record with timestamp 1000"))
        );
    }

    #[rstest]
    #[test]
    fn test_begin_read_early(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        let writer = entry.begin_read(1000);
        assert_eq!(
            writer.err(),
            Some(ReductError::not_found("No record with timestamp 1000"))
        );
    }

    #[rstest]
    #[test]
    fn test_begin_read_late(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        let writer = entry.begin_read(2000000);
        assert_eq!(
            writer.err(),
            Some(ReductError::not_found("No record with timestamp 2000000"))
        );
    }

    #[rstest]
    #[test]
    fn test_begin_read_still_written(mut entry: Entry) {
        {
            let writer = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .unwrap();
            writer
                .write()
                .unwrap()
                .write(Chunk::Data(Bytes::from(vec![0; 5])))
                .unwrap();
        }
        let reader = entry.begin_read(1000000);
        assert_eq!(
            reader.err(),
            Some(ReductError::too_early(
                "Record with timestamp 1000000 is still being written"
            ))
        );
    }

    #[rstest]
    #[test]
    fn test_begin_read_not_found(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        write_stub_record(&mut entry, 3000000).unwrap();

        let reader = entry.begin_read(2000000);
        assert_eq!(
            reader.err(),
            Some(ReductError::not_found("No record with timestamp 2000000"))
        );
    }

    #[rstest]
    #[test]
    fn test_begin_read_ok1(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        let reader = entry.begin_read(1000000).unwrap();
        let chunk = reader.write().unwrap().read().unwrap();
        assert_eq!(chunk.unwrap(), "0123456789".as_bytes());
    }

    #[rstest]
    #[test]
    fn test_begin_read_ok2(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        write_stub_record(&mut entry, 1010000).unwrap();

        let reader = entry.begin_read(1010000).unwrap();
        let chunk = reader.write().unwrap().read().unwrap();
        assert_eq!(chunk.unwrap(), "0123456789".as_bytes());
    }

    #[rstest]
    #[test]
    fn test_begin_read_ok_in_chunks(mut entry: Entry) {
        let mut data = vec![0; DEFAULT_MAX_READ_CHUNK as usize + 1];
        data[0] = 1;
        data[DEFAULT_MAX_READ_CHUNK as usize] = 2;

        write_record(&mut entry, 1000000, data.clone()).unwrap();

        let reader = entry.begin_read(1000000).unwrap();
        let mut wr = reader.write().unwrap();
        let chunk = wr.read().unwrap();
        assert_eq!(
            chunk.unwrap().to_vec(),
            data[0..DEFAULT_MAX_READ_CHUNK as usize]
        );
        let chunk = wr.read().unwrap();
        assert_eq!(
            chunk.unwrap().to_vec(),
            data[DEFAULT_MAX_READ_CHUNK as usize..]
        );
        let chunk = wr.read().unwrap();
        assert_eq!(chunk, None);
    }

    #[rstest]
    #[test]
    fn test_historical_query(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();
        write_stub_record(&mut entry, 2000000).unwrap();
        write_stub_record(&mut entry, 3000000).unwrap();

        let id = entry.query(0, 4000000, QueryOptions::default()).unwrap();
        assert!(id >= 1);

        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.read().unwrap().timestamp(), 1000000);
        }
        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.read().unwrap().timestamp(), 2000000);
        }
        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.read().unwrap().timestamp(), 3000000);
        }

        assert_eq!(
            entry.next(id).err(),
            Some(ReductError::no_content("No content"))
        );
        assert_eq!(
            entry.next(id).err(),
            Some(ReductError::not_found(&format!("Query {} not found", id)))
        );
    }

    #[rstest]
    #[test]
    fn test_continuous_query(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).unwrap();

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
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.read().unwrap().timestamp(), 1000000);
        }

        assert_eq!(
            entry.next(id).err(),
            Some(ReductError::no_content("No content"))
        );

        write_stub_record(&mut entry, 2000000).unwrap();
        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.read().unwrap().timestamp(), 2000000);
        }

        sleep(Duration::from_millis(600));
        assert_eq!(
            entry.next(id).err(),
            Some(ReductError::not_found(&format!("Query {} not found", id)))
        );
    }

    #[rstest]
    #[test]
    fn test_info(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 10000,
            },
            path,
        );

        write_stub_record(&mut entry, 1000000).unwrap();
        write_stub_record(&mut entry, 2000000).unwrap();
        write_stub_record(&mut entry, 3000000).unwrap();

        let info = entry.info().unwrap();
        assert_eq!(info.name, "entry");
        assert_eq!(info.size, 30);
        assert_eq!(info.record_count, 3);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1000000);
        assert_eq!(info.latest_record, 3000000);
    }

    #[rstest]
    #[test]
    fn test_search(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 5,
            },
            path,
        );

        let step = 100000;
        for i in 0..100 {
            write_stub_record(&mut entry, i * step).unwrap();
        }

        let reader = entry.begin_read(30 * step).unwrap();
        let wr = reader.write().unwrap();

        assert_eq!(wr.timestamp(), 3000000);
    }

    #[rstest]
    #[test]
    fn test_try_remove_block_from_empty_entry(mut entry: Entry) {
        assert_eq!(
            entry.try_remove_oldest_block(),
            Err(ReductError::internal_server_error("No block to remove"))
        );
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

    fn write_record(entry: &mut Entry, time: u64, data: Vec<u8>) -> Result<(), ReductError> {
        let writer = entry.begin_write(
            time,
            data.len() as u64,
            "text/plain".to_string(),
            Labels::new(),
        )?;
        let x = writer
            .write()
            .unwrap()
            .write(Chunk::Last(Bytes::from(data)));
        x
    }

    fn write_stub_record(entry: &mut Entry, time: u64) -> Result<(), ReductError> {
        write_record(entry, time, b"0123456789".to_vec())
    }
}
