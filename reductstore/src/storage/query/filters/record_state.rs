// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::proto::record::State;
use crate::storage::proto::Record;
use crate::storage::query::filters::RecordFilter;

pub struct RecordStateFilter {
    state: State,
}

impl RecordStateFilter {
    pub fn new(state: State) -> RecordStateFilter {
        RecordStateFilter { state }
    }
}

impl RecordFilter for RecordStateFilter {
    fn filter(&mut self, record: &Record) -> bool {
        record.state == self.state as i32
    }
}
