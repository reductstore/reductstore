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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::Record;
    use rstest::*;

    #[rstest]
    fn test_include_label_filter() {
        let mut filter = IncludeLabelFilter::new(Labels::from_iter(vec![(
            "key".to_string(),
            "value".to_string(),
        )]));
        let record = Record {
            labels: vec![Label {
                name: "key".to_string(),
                value: "value".to_string(),
            }],
            ..Default::default()
        };

        assert!(filter.filter(&record), "Record should pass");
    }

    #[rstest]
    fn test_include_label_filter_no_records() {
        let mut filter = IncludeLabelFilter::new(Labels::from_iter(vec![(
            "key".to_string(),
            "value".to_string(),
        )]));
        let record = Record {
            labels: vec![Label {
                name: "key".to_string(),
                value: "other".to_string(),
            }],
            ..Default::default()
        };

        assert!(!filter.filter(&record), "Record should not pass");
    }

    #[rstest]
    fn test_include_label_all() {
        let mut filter = IncludeLabelFilter::new(Labels::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let record = Record {
            labels: vec![
                Label {
                    name: "key1".to_string(),
                    value: "value1".to_string(),
                },
                Label {
                    name: "key2".to_string(),
                    value: "value2".to_string(),
                },
                Label {
                    name: "key3".to_string(),
                    value: "value3".to_string(),
                },
            ],
            ..Default::default()
        };

        assert!(
            filter.filter(&record),
            "Record should pass because it has key1=value1 and key2=value2"
        );

        let record = Record {
            labels: vec![Label {
                name: "key1".to_string(),
                value: "value1".to_string(),
            }],
            ..Default::default()
        };

        assert!(
            !filter.filter(&record),
            "Record should not pass because it has only key1=value1"
        );
    }
}
