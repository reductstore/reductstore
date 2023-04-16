// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use crate::storage::block_manager::{BlockManager, ManageBlock, DESCRIPTOR_FILE_EXT};
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block, Record};
use crate::storage::writer::RecordWriter;
use log::debug;
use prost::bytes::Bytes;
use prost::Message;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

type Labels = HashMap<String, String>;

/// Entry is a time series in a bucket.
pub struct Entry {
    name: String,
    path: PathBuf,
    options: EntryOptions,
    block_index: BTreeSet<u64>,
    block_manager: BlockManager,
    record_count: u64,
    size: u64,
}

/// EntryOptions is the options for creating a new entry.
#[derive(PartialEq, Debug, Clone)]
pub struct EntryOptions {
    pub max_block_size: u64,
    pub max_block_records: u64,
}

impl Entry {
    pub fn new(name: &str, path: PathBuf, options: EntryOptions) -> Result<Self, HTTPError> {
        fs::create_dir_all(path.join(name))?;
        Ok(Self {
            name: name.to_string(),
            path: path.clone(),
            options,
            block_index: BTreeSet::new(),
            block_manager: BlockManager::new(path.join(name)),
            record_count: 0,
            size: 0,
        })
    }

    pub fn restore(path: PathBuf, options: EntryOptions) -> Result<Self, HTTPError> {
        let mut record_count = 0;
        let mut size = 0;
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
        }

        Ok(Self {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            path: path.clone(),
            options,
            block_index: BTreeSet::new(),
            block_manager: BlockManager::new(path),
            record_count,
            size,
        })
    }

    pub fn begin_write(
        &mut self,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<RecordWriter, HTTPError> {
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
                    let prev = self.block_index.range(time..).next().unwrap();
                    let block = self.block_manager.load(*prev)?;
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

        let has_no_space = block.size + content_size > self.options.max_block_size;
        let has_too_many_records =
            block.records.len() + 1 >= self.options.max_block_records as usize;

        let mut block = if record_type == RecordType::Latest
            && (has_no_space || has_too_many_records || block.invalid)
        {
            // We need to create a new block.
            debug!("Creating a new block");
            self.block_manager.finish(block.as_ref())?;
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

        let mut block = Rc::make_mut(&mut block);

        block.size += content_size as u64;
        block.records.push(record);

        self.record_count += 1;
        self.size += content_size as u64;
        if record_type != RecordType::Belated {
            block.latest_record_time = Some(us_to_ts(&time));
        }

        self.block_manager.save(block)?;
        self.block_manager
            .begin_write(block, block.records.len() - 1)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn start_new_block(&mut self, time: u64) -> Result<Rc<Block>, HTTPError> {
        let block = self
            .block_manager
            .start(time, self.options.max_block_size)?;
        self.block_index
            .insert(ts_to_us(&block.begin_time.as_ref().unwrap()));
        Ok::<Rc<Block>, HTTPError>(block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn test_restore() {
        let (options, mut entry) = setup(EntryOptions {
            max_block_size: 10000,
            max_block_records: 10000,
        });

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
    fn test_new_block_size() {
        let (_, mut entry) = setup(EntryOptions {
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
    fn test_new_block_records() {
        let (_, mut entry) = setup(EntryOptions {
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
    fn test_belated_record() {
        let (_, mut entry) = setup(EntryOptions {
            max_block_size: 10000,
            max_block_records: 10000,
        });

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
    fn test_belated_first() {
        let (_, mut entry) = setup(EntryOptions {
            max_block_size: 10000,
            max_block_records: 10000,
        });

        write_record(&mut entry, 3000000, 10).unwrap();
        write_record(&mut entry, 1000000, 10).unwrap();

        let records = entry.block_manager.load(1000000).unwrap().records.clone();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].timestamp, Some(us_to_ts(&1000000)));
    }

    #[test]
    fn test_existing_record() {
        let (_, mut entry) = setup(EntryOptions {
            max_block_size: 10000,
            max_block_records: 10000,
        });

        write_record(&mut entry, 1000000, 10).unwrap();
        let err = write_record(&mut entry, 1000000, 10);
        assert_eq!(
            err.err(),
            Some(HTTPError::conflict(
                "A record with timestamp 1000000 already exists"
            ))
        );
    }

    fn setup(options: EntryOptions) -> (EntryOptions, Entry) {
        let path = tempfile::tempdir().unwrap();
        let entry = Entry::new("entry", path.into_path(), options.clone()).unwrap();
        (options, entry)
    }

    fn write_record(entry: &mut Entry, time: u64, content_size: usize) -> Result<(), HTTPError> {
        let mut writer = entry.begin_write(
            time,
            content_size as u64,
            "text/plain".to_string(),
            Labels::new(),
        )?;
        writer.write(&vec![0; content_size], true)
    }
}
