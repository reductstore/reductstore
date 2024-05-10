// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use reduct_base::Labels;

use crate::storage::proto::Record;
use crate::storage::query::filters::RecordFilter;

pub struct ExcludeLabelFilter {
    labels: Labels,
}

impl ExcludeLabelFilter {
    pub fn new(labels: Labels) -> ExcludeLabelFilter {
        ExcludeLabelFilter { labels }
    }
}

impl RecordFilter for ExcludeLabelFilter {
    fn filter(&self, record: &Record) -> bool {
        !self.labels.iter().all(|(key, value)| {
            record
                .labels
                .iter()
                .any(|label| label.name == *key && label.value == *value)
        })
    }

    fn last_sent(&mut self, _: &Record) -> () {
        ()
    }
}
