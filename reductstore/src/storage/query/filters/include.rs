// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use reduct_base::Labels;

use crate::storage::proto::Record;
use crate::storage::query::filters::RecordFilter;

pub struct IncludeLabelFilter {
    labels: Labels,
}

impl IncludeLabelFilter {
    pub fn new(labels: Labels) -> IncludeLabelFilter {
        IncludeLabelFilter { labels }
    }
}

impl RecordFilter for IncludeLabelFilter {
    fn filter(&mut self, record: &Record) -> bool {
        self.labels.iter().all(|(key, value)| {
            record
                .labels
                .iter()
                .any(|label| label.name == *key && label.value == *value)
        })
    }
}
