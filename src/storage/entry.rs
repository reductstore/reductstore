// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use crate::storage::block_manager::{BlockManager, ManageBlock, DESCRIPTOR_FILE_EXT};
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block, EntryInfo, Record};
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::build_query;
use crate::storage::reader::RecordReader;
use crate::storage::writer::RecordWriter;
use log::debug;
use prost::bytes::Bytes;
use prost::Message;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

pub type Labels = HashMap<String, String>;

/// Entry is a time series in a bucket.
pub struct Entry {
    name: String,
    path: PathBuf,
    settings: EntrySettings,
    block_index: BTreeSet<u64>,
    block_manager: BlockManager,
    record_count: u64,
    size: u64,
    queries: HashMap<u64, Box<dyn Query>>,
}

/// EntryOptions is the options for creating a new entry.
#[derive(PartialEq, Debug, Clone)]
pub struct EntrySettings {
    pub max_block_size: u64,
    pub max_block_records: u64,
}

impl Entry {
    pub fn new(name: &str, path: PathBuf, settings: EntrySettings) -> Result<Self, HTTPError> {
        fs::create_dir_all(path.join(name))?;
        Ok(Self {
            name: name.to_string(),
            path: path.clone(),
            settings,
            block_index: BTreeSet::new(),
            block_manager: BlockManager::new(path.join(name)),
            record_count: 0,
            size: 0,
            queries: HashMap::new(),
        })
    }

    pub fn restore(path: PathBuf, options: EntrySettings) -> Result<Self, HTTPError> {
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

            let buf = std::fs::read(path)?;
            let block = Block::decode(Bytes::from(buf)).map_err(|e| {
                HTTPError::internal_server_error(&format!(
                    "Failed to decode block descriptor: {}",
                    e
                ))
            })?;

            record_count += block.records.len() as u64;
            size += block.size;
            block_index.insert(ts_to_us(block.begin_time.as_ref().unwrap()));
        }

        Ok(Self {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            path: path.clone(),
            settings: options,
            block_index,
            block_manager: BlockManager::new(path),
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
    ) -> Result<Rc<RefCell<RecordWriter>>, HTTPError> {
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
            self.block_manager.load(*self.block_index.last().unwrap())?
        };

        let (block, record_type) =
            if block.begin_time.is_some() && ts_to_us(block.begin_time.as_ref().unwrap()) >= time {
                debug!("Timestamp {} is belated. Finding proper block", time);
                // The timestamp is belated. We need to find the proper block to write to.

                if *self.block_index.first().unwrap() > time {
                    // The timestamp is the earliest. We need to create a new block.
                    debug!("Timestamp {} is the earliest. Creating a new block", time);
                    (self.start_new_block(time)?, RecordType::BelatedFirst)
                } else {
                    // The timestamp is in the middle. We need to find the proper block.
                    debug!(
                        "Timestamp {} is in the middle. Finding the proper block",
                        time
                    );
                    let block_id = self.block_index.range(time..).next().unwrap();
                    let block = self.block_manager.load(*block_id)?;
                    // check if the record already exists
                    let proto_time = Some(us_to_ts(&time));
                    if block
                        .records
                        .iter()
                        .any(|record| record.timestamp == proto_time)
                    {
                        return Err(HTTPError::conflict(&format!(
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
            self.block_manager.finish(&block)?;
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

        self.block_manager.save(&block)?;
        self.block_manager
            .begin_write(&block, block.records.len() - 1)
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
    pub(crate) fn begin_read(&mut self, time: u64) -> Result<Rc<RefCell<RecordReader>>, HTTPError> {
        debug!("Reading record for ts={}", time);
        let not_found_err = Err(HTTPError::not_found(&format!(
            "No record with timestamp {}",
            time
        )));

        if self.block_index.is_empty() {
            return not_found_err;
        }

        if &time > self.block_index.last().unwrap() || &time < self.block_index.first().unwrap() {
            return not_found_err;
        }

        let block_id = self.block_index.range(time..).next().unwrap();
        let block = self.block_manager.load(*block_id)?;

        let record_index = block
            .records
            .iter()
            .position(|record| ts_to_us(record.timestamp.as_ref().unwrap()) == time);

        if record_index.is_none() {
            return not_found_err;
        }

        let record = &block.records[record_index.unwrap()];
        if record.state == record::State::Started as i32 {
            return Err(HTTPError::too_early(&format!(
                "Record with timestamp {} is still being written",
                time
            )));
        }

        if record.state == record::State::Errored as i32 {
            return Err(HTTPError::internal_server_error(&format!(
                "Record with timestamp {} is broken",
                time
            )));
        }

        self.block_manager.begin_read(&block, record_index.unwrap())
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
    pub fn query(&mut self, start: u64, end: u64, options: QueryOptions) -> Result<u64, HTTPError> {
        static QUERY_ID: AtomicU64 = AtomicU64::new(0);

        let id = QUERY_ID.fetch_add(1, Ordering::Relaxed);
        self.remove_expired_query();
        self.queries.insert(id, build_query(start, end, options));

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
    pub fn next(&mut self, query_id: u64) -> Result<(Rc<RefCell<RecordReader>>, bool), HTTPError> {
        self.remove_expired_query();
        let query = self
            .queries
            .get_mut(&query_id)
            .ok_or_else(|| HTTPError::not_found(&format!("Query {} not found", query_id)))?;
        query.next(&self.block_index, &mut self.block_manager)
    }

    /// Returns stats about the entry.
    pub fn info(&self) -> Result<EntryInfo, HTTPError> {
        let (oldest_record, latest_record) = if self.block_index.is_empty() {
            (0, 0)
        } else {
            let latest_block = self.block_manager.load(*self.block_index.last().unwrap())?;
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
    pub fn try_remove_oldest_block(&mut self) -> Result<(), HTTPError> {
        if self.block_index.is_empty() {
            return Ok(());
        }

        let oldest_block_id = *self.block_index.first().unwrap();

        let block = self.block_manager.load(oldest_block_id)?;
        self.size -= block.size;
        self.record_count -= block.records.len() as u64;

        self.block_manager.remove(oldest_block_id)?;
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

    fn start_new_block(&mut self, time: u64) -> Result<Block, HTTPError> {
        let block = self
            .block_manager
            .start(time, self.settings.max_block_size)?;
        self.block_index
            .insert(ts_to_us(&block.begin_time.as_ref().unwrap()));
        Ok::<Block, HTTPError>(block)
    }

    fn remove_expired_query(&mut self) {
        self.queries
            .retain(|_, query| query.state() == &QueryState::Running);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_manager::DEFAULT_MAX_READ_CHUNK;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile;

    #[test]
    fn test_restore() {
        let (options, mut entry) = setup_default();
        write_record(&mut entry, 1, 10).unwrap();
        write_record(&mut entry, 2000010, 10).unwrap();

        let records = entry.block_manager.load(1).unwrap().records.clone();
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

        let entry = Entry::restore(entry.path.join(entry.name), options).unwrap();

        assert_eq!(entry.name(), "entry");
        assert_eq!(entry.record_count, 2);
        assert_eq!(entry.size, 20);
    }

    #[test]
    fn test_begin_write_new_block_size() {
        let (_, mut entry) = setup(EntrySettings {
            max_block_size: 10,
            max_block_records: 10000,
        });

        write_record(&mut entry, 1, 10).unwrap();
        write_record(&mut entry, 2000010, 10).unwrap();

        assert_eq!(
            entry.block_manager.load(1).unwrap().records[0],
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
            entry.block_manager.load(2000010).unwrap().records[0],
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

    #[test]
    fn test_begin_write_new_block_records() {
        let (_, mut entry) = setup(EntrySettings {
            max_block_size: 10000,
            max_block_records: 1,
        });

        write_record(&mut entry, 1, 10).unwrap();
        write_record(&mut entry, 2, 10).unwrap();
        write_record(&mut entry, 2000010, 10).unwrap();

        assert_eq!(
            entry.block_manager.load(1).unwrap().records[0],
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
            entry.block_manager.load(2000010).unwrap().records[0],
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

    #[test]
    fn test_begin_write_belated_record() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 1000000, 10).unwrap();
        write_record(&mut entry, 3000000, 10).unwrap();
        write_record(&mut entry, 2000000, 10).unwrap();

        let records = entry.block_manager.load(1000000).unwrap().records.clone();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
        assert_eq!(records[1].timestamp, Some(us_to_ts(&3000000)));
        assert_eq!(records[2].timestamp, Some(us_to_ts(&2000000)));
    }

    #[test]
    fn test_begin_write_belated_first() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 3000000, 10).unwrap();
        write_record(&mut entry, 1000000, 10).unwrap();

        let records = entry.block_manager.load(1000000).unwrap().records.clone();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
    }

    #[test]
    fn test_begin_write_existing_record() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 1000000, 10).unwrap();
        let err = write_record(&mut entry, 1000000, 10);
        assert_eq!(
            err.err(),
            Some(HTTPError::conflict(
                "A record with timestamp 1000000 already exists"
            ))
        );
    }

    // Test begin_read
    #[test]
    fn test_begin_read_empty() {
        let (_, mut entry) = setup_default();
        let writer = entry.begin_read(1000);
        assert_eq!(
            writer.err(),
            Some(HTTPError::not_found("No record with timestamp 1000"))
        );
    }

    #[test]
    fn test_begin_read_early() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 1000000, 10).unwrap();
        let writer = entry.begin_read(1000);
        assert_eq!(
            writer.err(),
            Some(HTTPError::not_found("No record with timestamp 1000"))
        );
    }

    #[test]
    fn test_begin_read_late() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 1000000, 10).unwrap();
        let writer = entry.begin_read(2000000);
        assert_eq!(
            writer.err(),
            Some(HTTPError::not_found("No record with timestamp 2000000"))
        );
    }

    #[test]
    fn test_begin_read_still_written() {
        let (_, mut entry) = setup_default();
        {
            let writer = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .unwrap();
            writer.borrow_mut().write(&vec![0; 5], false).unwrap();
        }
        let reader = entry.begin_read(1000000);
        assert_eq!(
            reader.err(),
            Some(HTTPError::too_early(
                "Record with timestamp 1000000 is still being written"
            ))
        );
    }

    #[test]
    fn test_begin_read_not_found() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 1000000, 10).unwrap();
        write_record(&mut entry, 3000000, 10).unwrap();

        let reader = entry.begin_read(2000000);
        assert_eq!(
            reader.err(),
            Some(HTTPError::not_found("No record with timestamp 2000000"))
        );
    }

    #[test]
    fn test_begin_read_ok() {
        let (_, mut entry) = setup_default();
        {
            let writer = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .unwrap();
            writer
                .borrow_mut()
                .write("0123456789".as_bytes(), true)
                .unwrap();
        }
        let reader = entry.begin_read(1000000).unwrap();
        let chunk = reader.borrow_mut().read().unwrap();
        assert_eq!(chunk.data, "0123456789".as_bytes());
        assert!(chunk.last);
    }

    #[test]
    fn test_begin_read_ok_in_chunks() {
        let (_, mut entry) = setup_default();
        let mut data = vec![0; DEFAULT_MAX_READ_CHUNK as usize + 1];
        data[0] = 1;
        data[DEFAULT_MAX_READ_CHUNK as usize] = 2;

        {
            let writer = entry
                .begin_write(
                    1000000,
                    data.len() as u64,
                    "text/plain".to_string(),
                    Labels::new(),
                )
                .unwrap();
            writer.borrow_mut().write(&data, true).unwrap();
        }
        let reader = entry.begin_read(1000000).unwrap();
        let chunk = reader.borrow_mut().read().unwrap();
        assert_eq!(chunk.data, data[0..DEFAULT_MAX_READ_CHUNK as usize]);
        assert!(!chunk.last);

        let chunk = reader.borrow_mut().read().unwrap();
        assert_eq!(chunk.data, data[DEFAULT_MAX_READ_CHUNK as usize..]);
        assert!(chunk.last);
    }

    #[test]
    fn test_historical_query() {
        let (_, mut entry) = setup_default();

        write_record(&mut entry, 1000000, 10).unwrap();
        write_record(&mut entry, 2000000, 10).unwrap();
        write_record(&mut entry, 3000000, 10).unwrap();

        let id = entry.query(0, 4000000, QueryOptions::default()).unwrap();

        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.borrow().timestamp(), 1000000);
        }
        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.borrow_mut().timestamp(), 2000000);
        }
        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.borrow().timestamp(), 3000000);
        }

        assert_eq!(
            entry.next(id).err(),
            Some(HTTPError::no_content("No content"))
        );
        assert_eq!(
            entry.next(id).err(),
            Some(HTTPError::not_found(&format!("Query {} not found", id)))
        );
    }

    #[test]
    fn test_continuous_query() {
        let (_, mut entry) = setup_default();
        write_record(&mut entry, 1000000, 10).unwrap();

        let id = entry
            .query(
                0,
                4000000,
                QueryOptions {
                    ttl: Duration::from_millis(100),
                    continuous: true,
                    ..QueryOptions::default()
                },
            )
            .unwrap();

        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.borrow().timestamp(), 1000000);
        }

        assert_eq!(
            entry.next(id).err(),
            Some(HTTPError::no_content("No content"))
        );

        write_record(&mut entry, 2000000, 10).unwrap();
        {
            let (reader, _) = entry.next(id).unwrap();
            assert_eq!(reader.borrow().timestamp(), 2000000);
        }

        sleep(Duration::from_millis(200));
        assert_eq!(
            entry.next(id).err(),
            Some(HTTPError::not_found(&format!("Query {} not found", id)))
        );
    }

    #[test]
    fn test_info() {
        let (_, mut entry) = setup(EntrySettings {
            max_block_size: 10000,
            max_block_records: 10000,
        });

        write_record(&mut entry, 1000000, 10).unwrap();
        write_record(&mut entry, 2000000, 10).unwrap();
        write_record(&mut entry, 3000000, 10).unwrap();

        let info = entry.info().unwrap();
        assert_eq!(info.name, "entry");
        assert_eq!(info.size, 30);
        assert_eq!(info.record_count, 3);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1000000);
        assert_eq!(info.latest_record, 3000000);
    }

    fn setup(options: EntrySettings) -> (EntrySettings, Entry) {
        let path = tempfile::tempdir().unwrap();
        let entry = Entry::new("entry", path.into_path(), options.clone()).unwrap();
        (options, entry)
    }

    fn setup_default() -> (EntrySettings, Entry) {
        setup(EntrySettings {
            max_block_size: 10000,
            max_block_records: 10000,
        })
    }

    fn write_record(entry: &mut Entry, time: u64, content_size: usize) -> Result<(), HTTPError> {
        let writer = entry.begin_write(
            time,
            content_size as u64,
            "text/plain".to_string(),
            Labels::new(),
        )?;
        let ret = writer.borrow_mut().write(&vec![0; content_size], true);
        ret
    }
}