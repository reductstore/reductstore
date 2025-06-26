// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reduct_base::error::ReductError;
use reduct_base::Labels;

use crate::storage::query::filters::{FilterRecord, RecordFilter, RecordMeta};

/// Filter that excludes records with specific labels
pub struct IncludeLabelFilter {
    labels: Labels,
}

impl IncludeLabelFilter {
    /// Create a new filter that includes records with the specified labels
    ///
    /// # Arguments
    ///
    /// * `labels` - The labels to include
    ///
    /// # Returns
    ///
    /// A new `IncludeLabelFilter` instance
    pub fn new(labels: Labels) -> IncludeLabelFilter {
        IncludeLabelFilter { labels }
    }
}

impl<R: FilterRecord> RecordFilter<R> for IncludeLabelFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        let result = self.labels.iter().all(|(key, value)| {
            record
                .labels()
                .iter()
                .any(|(k, v)| *k == key && *v == value)
        });

        if result {
            Ok(Some(vec![record]))
        } else {
            Ok(Some(vec![]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::query::filters::tests::TestFilterRecord;
    use rstest::*;

    #[rstest]
    fn test_include_label_filter() {
        let mut filter = IncludeLabelFilter::new(Labels::from_iter(vec![(
            "key".to_string(),
            "value".to_string(),
        )]));

        let record: TestFilterRecord = RecordMeta::builder()
            .labels(Labels::from_iter(vec![(
                "key".to_string(),
                "value".to_string(),
            )]))
            .build()
            .into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record]),
            "Record should pass"
        );
    }

    #[rstest]
    fn test_include_label_filter_no_records() {
        let mut filter = IncludeLabelFilter::new(Labels::from_iter(vec![(
            "key".to_string(),
            "value".to_string(),
        )]));
        let record: TestFilterRecord = RecordMeta::builder()
            .labels(Labels::from_iter(vec![(
                "key".to_string(),
                "other".to_string(),
            )]))
            .build()
            .into();
        assert_eq!(
            filter.filter(record).unwrap(),
            Some(vec![]),
            "Record should not pass"
        );
    }

    #[rstest]
    fn test_include_label_all() {
        let mut filter = IncludeLabelFilter::new(Labels::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let record: TestFilterRecord = RecordMeta::builder()
            .labels(Labels::from_iter(vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
                ("key3".to_string(), "value3".to_string()),
            ]))
            .build()
            .into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record]),
            "Record should pass because it has key1=value1 and key2=value2"
        );

        let record: TestFilterRecord = RecordMeta::builder()
            .labels(Labels::from_iter(vec![(
                "key1".to_string(),
                "value1".to_string(),
            )]))
            .build()
            .into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![]),
            "Record should not pass because it has only key1=value1"
        );
    }
}
