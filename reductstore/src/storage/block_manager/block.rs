// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::proto::{ts_to_us, us_to_ts, Block as BlockProto, Record};
use prost::Message;
use reduct_base::error::{ErrorCode, ReductError};
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

    pub fn insert_record(&mut self, record: Record) -> Result<(), ReductError> {
        match self
            .record_index
            .entry(ts_to_us(&record.timestamp.unwrap()))
        {
            Entry::Occupied(_) => Err(ReductError::new(
                ErrorCode::Conflict,
                "Record already exists",
            )),
            Entry::Vacant(entry) => {
                self.size += record.end - record.begin;
                self.record_count += 1;
                self.metadata_size += record.encoded_len() as u64;
                entry.insert(record);
                Ok(())
            }
        }
    }

    pub fn get_record(&self, timestamp: u64) -> Option<&Record> {
        self.record_index.get(&timestamp)
    }

    pub fn get_record_mut(&mut self, timestamp: u64) -> Option<&mut Record> {
        self.record_index.get_mut(&timestamp)
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
}