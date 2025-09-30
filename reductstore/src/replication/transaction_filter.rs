// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::replication::TransactionNotification;
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::{
    apply_filters_recursively, EachNFilter, EachSecondFilter, ExcludeLabelFilter, FilterRecord,
    IncludeLabelFilter, RecordFilter, WhenFilter,
};
use log::warn;
use reduct_base::error::ReductError;
use reduct_base::msg::replication_api::ReplicationSettings;
use std::collections::HashMap;

type Filter = Box<dyn RecordFilter<TransactionNotification> + Send + Sync>;
/// Filter for transaction notifications.
pub(super) struct TransactionFilter {
    bucket: String,
    entries: Vec<String>,
    query_filters: Vec<Filter>,
}

impl FilterRecord for TransactionNotification {
    fn timestamp(&self) -> u64 {
        *self.event.timestamp()
    }

    fn labels(&self) -> HashMap<&String, &String> {
        self.meta.labels().iter().map(|(k, v)| (k, v)).collect()
    }

    fn set_labels(&mut self, labels: HashMap<String, String>) {
        let labels_mut = self.meta.labels_mut();
        labels_mut.clear();
        labels_mut.extend(labels);
    }

    fn computed_labels(&self) -> HashMap<&String, &String> {
        self.meta
            .computed_labels()
            .iter()
            .map(|(k, v)| (k, v))
            .collect()
    }

    fn state(&self) -> i32 {
        self.meta.state()
    }
}

impl TransactionFilter {
    /// Create a new transaction filter.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name to filter.
    /// * `entries` - Entries to filter. Supports wildcards. If empty, all entries are matched.
    /// * `include` - Labels to include. All must match. If empty, all labels are matched.
    /// * `exclude` - Labels to exclude. Any must match. If empty, no labels are matched.
    pub(super) fn try_new(
        name: &str,
        settings: ReplicationSettings,
        io_config: IoConfig,
    ) -> Result<Self, ReductError> {
        let mut query_filters: Vec<Filter> = vec![];
        if !settings.include.is_empty() {
            query_filters.push(Box::new(IncludeLabelFilter::new(settings.include)));
        }

        if !settings.exclude.is_empty() {
            query_filters.push(Box::new(ExcludeLabelFilter::new(settings.exclude)));
        }

        if let Some(each_n) = settings.each_n {
            query_filters.push(Box::new(EachNFilter::new(each_n)));
        }

        if let Some(each_s) = settings.each_s {
            query_filters.push(Box::new(EachSecondFilter::new(each_s)));
        }

        if let Some(when) = settings.when {
            match Parser::new().parse(when.clone()) {
                Ok((condition, directives)) => {
                    query_filters.push(Box::new(WhenFilter::try_new(
                        condition, directives, io_config, true,
                    )?));
                }
                Err(err) => warn!(
                    "Error parsing when condition in {} replication task: {}",
                    name, err
                ),
            }
        }

        Ok(Self {
            bucket: settings.src_bucket,
            entries: settings.entries,
            query_filters,
        })
    }

    /// Filter a transaction notification.
    ///
    /// # Arguments
    ///
    /// * `notification` - Transaction notification to filter.
    ///
    /// # Returns
    ///
    /// `true` if the notification matches the filter, `false` otherwise.
    pub(super) fn filter(
        &mut self,
        notification: TransactionNotification,
    ) -> Vec<TransactionNotification> {
        if notification.bucket != self.bucket {
            return vec![];
        }

        if !self.entries.is_empty() {
            let mut found = false;
            for entry in self.entries.iter() {
                if entry.contains('*') {
                    let entry = entry.replace("*", "");
                    if notification.entry.starts_with(&entry) {
                        found = true;
                        break;
                    }
                } else if notification.entry == *entry {
                    found = true;
                    break;
                }
            }
            if !found {
                return vec![];
            }
        }

        match apply_filters_recursively(
            self.query_filters.as_mut_slice(),
            vec![notification.clone()],
            0,
        ) {
            Ok(Some(notifications)) => notifications,
            Ok(None) => {
                warn!(
                    "Filtering interrupted in replication task '{}'",
                    self.bucket
                );
                vec![]
            }
            Err(err) => {
                warn!(
                    "Error applying filters in replication task '{}': {}",
                    self.bucket, err
                );
                vec![]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::Transaction;
    use reduct_base::io::RecordMeta;
    use reduct_base::Labels;
    use rstest::*;

    use super::*;

    #[rstest]
    fn test_transaction_filter(notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();
        assert_eq!(filter.filter(notification).len(), 1);
    }

    #[rstest]
    fn test_transaction_filter_bucket(notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "other".to_string(),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();
        assert_eq!(filter.filter(notification).len(), 0);
    }

    #[rstest]
    #[case(vec ! ["entry".to_string()], true)]
    #[case(vec ! ["other".to_string(), "entry".to_string()], true)]
    #[case(vec ! ["ent*".to_string()], true)]
    #[case(vec ! ["other".to_string()], false)]
    #[case(vec ! ["oth*".to_string()], false)]
    fn test_transaction_filter_entries(
        #[case] entries: Vec<String>,
        #[case] expected: bool,
        notification: TransactionNotification,
    ) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                entries,
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        let filtered = filter.filter(notification);
        assert_eq!(filtered.is_empty(), !expected);
    }

    #[rstest]
    #[case(vec ! [("a".to_string(), "b".to_string())], false)]
    #[case(vec ! [("x".to_string(), "z".to_string())], false)]
    #[case(vec ! [("x".to_string(), "y".to_string())], true)]
    #[case(vec ! [("x".to_string(), "y".to_string()), ("z".to_string(), "w".to_string())], true)]
    #[case(vec ! [("x".to_string(), "y".to_string()), ("z".to_string(), "z".to_string())], false)]
    #[case(vec ! [("x".to_string(), "y".to_string()), ("z".to_string(), "w".to_string()), ("a".to_string(), "b".to_string())], false)]
    fn test_transaction_filter_include(
        #[case] include: Vec<(String, String)>,
        #[case] expected: bool,
        notification: TransactionNotification,
    ) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                include: Labels::from_iter(include),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        let filtered = filter.filter(notification);
        assert_eq!(filtered.is_empty(), !expected);
    }

    #[rstest]
    #[case(vec ! [("a".to_string(), "b".to_string())], true)]
    #[case(vec ! [("x".to_string(), "z".to_string())], true)]
    #[case(vec ! [("x".to_string(), "y".to_string())], false)]
    #[case(vec ! [("x".to_string(), "y".to_string()), ("z".to_string(), "w".to_string())], false)]
    #[case(vec ! [("z".to_string(), "w".to_string())], false)]
    fn test_transaction_filter_exclude(
        #[case] exclude: Vec<(String, String)>,
        #[case] expected: bool,
        notification: TransactionNotification,
    ) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                exclude: Labels::from_iter(exclude),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        let filtered = filter.filter(notification);
        assert_eq!(filtered.is_empty(), !expected);
    }

    #[rstest]
    fn test_transaction_filter_each_n(notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                each_n: Some(2),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert_eq!(filter.filter(notification.clone()).len(), 1);
        assert_eq!(filter.filter(notification.clone()).len(), 0);
        assert_eq!(filter.filter(notification).len(), 1);
    }

    #[rstest]
    fn test_transaction_filter_each_s(mut notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                each_s: Some(1.0),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert_eq!(filter.filter(notification.clone()).len(), 1);
        notification.event = Transaction::WriteRecord(1);
        assert_eq!(filter.filter(notification.clone()).len(), 0);
        notification.event = Transaction::WriteRecord(2);
        assert_eq!(filter.filter(notification.clone()).len(), 0);
        notification.event = Transaction::WriteRecord(1000_002);
        assert_eq!(filter.filter(notification.clone()).len(), 1);
    }

    #[rstest]
    fn test_transaction_filter_when(notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&x", "y"]})),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert_eq!(filter.filter(notification.clone()).len(), 1);

        filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&x", "z"]})),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert_eq!(filter.filter(notification).len(), 0);
    }

    #[rstest]
    fn test_transaction_filter_when_non_strict(notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&NOT_EXIST", "y"]})),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert!(
            filter.filter(notification).is_empty(),
            "label doesn't exist but we consider it as false"
        );
    }

    #[rstest]
    fn test_transaction_filter_interrupted(notification: TransactionNotification) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$limit": 0})),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert!(
            filter.filter(notification).is_empty(),
            "label doesn't exist but we consider it as false"
        );
    }

    #[rstest]
    fn test_transaction_filter_invalid_when() {
        let filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$UNKNOWN_OP": ["&x", "y", "z"]})),
                ..ReplicationSettings::default()
            },
        )
        .unwrap();

        assert!(filter.query_filters.is_empty());
    }

    #[fixture]
    fn notification() -> TransactionNotification {
        let labels = Labels::from_iter(vec![
            ("x".to_string(), "y".to_string()),
            ("z".to_string(), "w".to_string()),
        ]);
        TransactionNotification {
            bucket: "bucket".to_string(),
            entry: "entry".to_string(),
            meta: RecordMeta::builder()
                .timestamp(0)
                .labels(labels)
                .computed_labels(Labels::default())
                .state(0)
                .build(),
            event: Transaction::WriteRecord(0),
        }
    }

    mod filter_record_impl {
        use super::*;

        #[rstest]
        fn test_filter_record_impl(notification: TransactionNotification) {
            let record: Box<dyn FilterRecord> = Box::new(notification.clone());
            assert_eq!(record.timestamp(), *notification.event.timestamp());

            for (key, value) in notification.meta.labels() {
                assert_eq!(record.labels().get(key), Some(&value));
            }

            for (key, value) in notification.meta.computed_labels() {
                assert_eq!(record.computed_labels().get(key), Some(&value));
            }

            assert_eq!(record.state(), notification.meta.state());
        }

        #[rstest]
        fn test_set_labels(mut notification: TransactionNotification) {
            let new_labels = HashMap::from([
                ("a".to_string(), "b".to_string()),
                ("c".to_string(), "d".to_string()),
            ]);

            notification.set_labels(new_labels.clone());

            assert_eq!(notification.labels().len(), 2);
            assert_eq!(
                notification.labels().get(&"a".to_string()),
                Some(&&"b".to_string())
            );
            assert_eq!(
                notification.labels().get(&"c".to_string()),
                Some(&&"d".to_string())
            );
        }
    }
}
