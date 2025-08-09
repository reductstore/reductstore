// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::Value;
use crate::storage::query::filters::FilterRecord;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::collections::{HashMap, HashSet};

pub(super) struct LabelSelector {
    labels: HashSet<String>,
}

impl LabelSelector {
    pub fn try_new(directive: Option<Vec<Value>>) -> Result<Self, ReductError> {
        let labels = match directive {
            Some(labels) => {
                if labels.is_empty() {
                    return Err(unprocessable_entity!(
                        "#select_labels must contain at least one label"
                    ));
                }

                for label in &labels {
                    if !label.is_string() {
                        return Err(unprocessable_entity!(
                            "#select_labels must contain only string values"
                        ));
                    }
                }

                labels
                    .into_iter()
                    .map(|label| label.to_string())
                    .collect::<HashSet<String>>()
            }
            None => HashSet::new(), // Default to no labels selected
        };

        Ok(LabelSelector { labels })
    }

    pub fn select_labels<F: FilterRecord>(&self, mut record: F) -> F {
        if !self.labels.is_empty() {
            let filtered_labels = record
                .labels()
                .into_iter()
                .filter(|(k, _)| self.labels.contains(k.as_str()))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>();

            record.set_labels(filtered_labels);
        }

        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::filters::tests::TestFilterRecord;
    use crate::storage::query::filters::FilterRecord;
    use reduct_base::io::RecordMeta;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};

    #[rstest]
    fn test_label_selector(record: TestFilterRecord) {
        let selector =
            LabelSelector::try_new(Some(vec![Value::String("label1".to_string())])).unwrap();

        let filtered_record = selector.select_labels(record);
        assert_eq!(filtered_record.labels().len(), 1);
        assert!(filtered_record.labels().contains_key(&"label1".to_string()));
    }

    #[rstest]
    fn test_label_selector_empty(record: TestFilterRecord) {
        let selector = LabelSelector::try_new(None).unwrap();

        let filtered_record = selector.select_labels(record.clone());
        assert_eq!(
            filtered_record.labels(),
            record.labels(),
            "should be unchanged"
        );
    }

    #[rstest]
    fn test_new_label_selector_invalid() {
        let result = LabelSelector::try_new(Some(vec![Value::Int(42)]));
        assert!(
            result.is_err(),
            "should return an error for non-string values"
        );
    }

    #[rstest]
    fn test_new_label_selector_empty() {
        let result = LabelSelector::try_new(Some(vec![]));
        assert!(
            result.is_err(),
            "should return an error for empty label list"
        );
    }

    #[fixture]
    fn record() -> TestFilterRecord {
        RecordMeta::builder()
            .timestamp(1234567890)
            .state(1)
            .labels(Labels::from_iter(vec![
                ("label1".to_string(), "value1".to_string()),
                ("label2".to_string(), "value2".to_string()),
            ]))
            .build()
            .into()
    }
}
