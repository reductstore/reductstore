// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::proto::{ts_to_us, us_to_ts, Block as BlockProto, Record};
use prost::Message;
use reduct_base::error::{ErrorCode, ReductError};
use std::cmp::min;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub(in crate::storage) struct Block {
    block_id: u64,
    size: u64,
    record_count: u64,
    metadata_size: u64,
    record_index: BTreeMap<u64, Record>,
}

impl From<BlockProto> for Block {
    fn from(inner: BlockProto) -> Self {
        let mut record_index = BTreeMap::new();
        for record in inner.records {
            record_index.insert(ts_to_us(&record.timestamp.unwrap()), record);
        }

        Block {
            block_id: ts_to_us(&inner.begin_time.unwrap()),
            size: inner.size,
            record_count: inner.record_count,
            metadata_size: inner.metadata_size,
            record_index,
        }
    }
}

impl From<Block> for BlockProto {
    fn from(block: Block) -> Self {
        let latest_record_time = us_to_ts(&block.latest_record_time());
        let mut records = Vec::new();
        for (_, record) in block.record_index {
            records.push(record);
        }

        BlockProto {
            begin_time: Some(us_to_ts(&block.block_id)),
            latest_record_time: Some(latest_record_time),
            size: block.size,
            record_count: block.record_count,
            metadata_size: block.metadata_size,
            records,
            invalid: false,
        }
    }
}

impl Block {
    pub fn new(block_id: u64) -> Self {
        Block {
            block_id,
            size: 0,
            record_count: 0,
            metadata_size: 0,
            record_index: BTreeMap::new(),
        }
    }

    pub fn insert_or_update_record(&mut self, record: Record) {
        match self
            .record_index
            .entry(ts_to_us(&record.timestamp.unwrap()))
        {
            Entry::Occupied(mut entry) => {
                let existing_record = entry.get_mut();
                self.size -= existing_record.end - existing_record.begin;
                self.metadata_size -= min(self.metadata_size, existing_record.encoded_len() as u64);

                *existing_record = record;
                self.size += existing_record.end - existing_record.begin;
                self.metadata_size += existing_record.encoded_len() as u64;
            }
            Entry::Vacant(entry) => {
                self.size += record.end - record.begin;
                self.record_count += 1;
                self.metadata_size += record.encoded_len() as u64;
                entry.insert(record);
            }
        }
    }

    pub fn get_record(&self, timestamp: u64) -> Option<&Record> {
        self.record_index.get(&timestamp)
    }

    pub fn record_index(&self) -> &BTreeMap<u64, Record> {
        &self.record_index
    }

    pub fn block_id(&self) -> u64 {
        self.block_id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn metadata_size(&self) -> u64 {
        self.metadata_size
    }

    pub fn latest_record_time(&self) -> u64 {
        self.record_index.keys().next_back().cloned().unwrap_or(0)
    }

    pub fn change_record_state(&mut self, timestamp: u64, state: i32) -> Result<(), ReductError> {
        match self.record_index.get_mut(&timestamp) {
            Some(record) => {
                record.state = state;
                Ok(())
            }
            None => Err(ReductError::new(ErrorCode::NotFound, "Record not found")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};

    #[rstest]
    fn test_into_proto(block: Block, block_proto: BlockProto) {
        assert_eq!(block_proto, block.into());
    }

    #[rstest]
    fn test_from_proto(block: Block, block_proto: BlockProto) {
        assert_eq!(block, Block::from(block_proto));
    }

    #[rstest]
    fn test_insert_record(mut block: Block, record: Record) {
        block.insert_or_update_record(record.clone());
        let mut record2 = record.clone();
        record2.timestamp = Some(us_to_ts(&2));
        block.insert_or_update_record(record2.clone());

        assert_eq!(block.get_record(1), Some(&record));
        assert_eq!(block.get_record(2), Some(&record2));

        assert_eq!(block.size(), 3);
        assert_eq!(block.record_count(), 2);
        assert_eq!(block.metadata_size(), 54);
        assert_eq!(block.latest_record_time(), 2);
    }

    #[rstest]
    fn test_update_record(mut block: Block, record: Record) {
        block.insert_or_update_record(record.clone());
        let mut updated_record = record.clone();
        updated_record.state = 1;
        updated_record.end = 100;
        updated_record.content_type = "application/xml".to_string();
        block.insert_or_update_record(updated_record.clone());

        assert_eq!(block.get_record(1), Some(&updated_record));
        assert_eq!(block.size(), 100);
        assert_eq!(block.record_count(), 1);
        assert_eq!(block.metadata_size(), 28);
        assert_eq!(block.latest_record_time(), 1);
    }

    #[fixture]
    fn record() -> Record {
        Record {
            begin: 1,
            end: 2,
            timestamp: Some(us_to_ts(&1)),
            labels: vec![],
            content_type: "application/json".to_string(),
            state: 0,
        }
    }

    #[fixture]
    fn block(record: Record) -> Block {
        Block {
            block_id: 1,
            size: 2,
            record_count: 1,
            metadata_size: 4,
            record_index: BTreeMap::from_iter(vec![(1, record)]),
        }
    }

    #[fixture]
    fn block_proto(record: Record) -> BlockProto {
        BlockProto {
            begin_time: Some(us_to_ts(&1)),
            latest_record_time: Some(us_to_ts(&1)),
            size: 2,
            record_count: 1,
            metadata_size: 4,
            records: vec![record],
            invalid: false,
        }
    }
}
