// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::io::IoConfig;
use crate::replication::TransactionNotification;
use crate::storage::entry::{entry_matches_pattern, is_system_meta_entry, meta_entry_parent};
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::{
    apply_filters_recursively, EachNFilter, ExcludeLabelFilter, FilterRecord, RecordFilter,
    WhenFilter,
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
        if !settings.exclude.is_empty() {
            query_filters.push(Box::new(ExcludeLabelFilter::new(settings.exclude)));
        }

        if let Some(each_n) = settings.each_n {
            query_filters.push(Box::new(EachNFilter::new(each_n)));
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

        if !Self::entry_is_selected(&self.entries, &notification.entry) {
            return vec![];
        }

        if is_system_meta_entry(&notification.entry) {
            return vec![notification];
        }

        let notifications = match apply_filters_recursively(
            self.query_filters.as_mut_slice(),
            vec![notification.clone()],
            0,
        ) {
            Ok(Some(notifications)) => notifications,
            Ok(None) => {
                warn!(
                    "Filtering interrupted in replication task '{}' for entry '{}'",
                    self.bucket, notification.entry
                );
                vec![]
            }
            Err(err) => {
                warn!(
                    "Error applying filters in replication task '{}' for entry '{}': {}",
                    self.bucket, notification.entry, err
                );
                vec![]
            }
        };
        notifications
    }

    fn entry_is_selected(entries: &[String], entry_name: &str) -> bool {
        if entries.is_empty() {
            return true;
        }

        let include_patterns: Vec<&str> = entries
            .iter()
            .filter_map(|pattern| {
                if pattern.starts_with('!') && pattern.len() > 1 {
                    None
                } else {
                    Some(pattern.as_str())
                }
            })
            .collect();
        let exclude_patterns: Vec<&str> = entries
            .iter()
            .filter_map(|pattern| pattern.strip_prefix('!'))
            .filter(|pattern| !pattern.is_empty())
            .collect();

        let included = include_patterns.is_empty()
            || include_patterns
                .iter()
                .any(|pattern| Self::entry_matches(pattern, entry_name));

        included
            && !exclude_patterns
                .iter()
                .any(|pattern| Self::entry_matches(pattern, entry_name))
    }

    fn entry_matches(entry_filter: &str, entry_name: &str) -> bool {
        let entry_filter = entry_filter.trim_start_matches('/');

        entry_matches_pattern(entry_name, entry_filter)
            || meta_entry_parent(entry_name)
                .is_some_and(|parent| entry_matches_pattern(parent, entry_filter))
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
            IoConfig::default(),
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
            IoConfig::default(),
        )
        .unwrap();
        assert_eq!(filter.filter(notification).len(), 0);
    }

    #[rstest]
    #[case("entry", vec ! ["entry".to_string()], true)]
    #[case("entry", vec ! ["other".to_string(), "entry".to_string()], true)]
    #[case("entry", vec ! ["ent*".to_string()], true)]
    #[case("entry", vec ! ["*".to_string()], true)]
    #[case("entry", vec ! ["other".to_string()], false)]
    #[case("entry", vec ! ["oth*".to_string()], false)]
    #[case("cam-1", vec ! ["cam-*".to_string()], true)]
    #[case("cam-1/nested", vec ! ["cam-*".to_string()], true)]
    #[case("a/x/b", vec ! ["/a/*/b".to_string()], true)]
    #[case("a/x/d/b", vec ! ["/a/*/b".to_string()], false)]
    #[case("a/x/b", vec ! ["/a/**/b".to_string()], true)]
    #[case("a/x/d/b", vec ! ["/a/**/b".to_string()], true)]
    #[case("a/private/x/b", vec ! ["!/a/private/**".to_string()], false)]
    #[case("a/public/x/b", vec ! ["!/a/private/**".to_string()], true)]
    #[case("a/private/x/b", vec ! ["/a/**/b".to_string(), "!/a/private/**".to_string()], false)]
    #[case("a/public/x/b", vec ! ["/a/**/b".to_string(), "!/a/private/**".to_string()], true)]
    fn test_transaction_filter_entries(
        #[case] entry: String,
        #[case] entries: Vec<String>,
        #[case] expected: bool,
        mut notification: TransactionNotification,
    ) {
        notification.entry = entry;
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                entries,
                ..ReplicationSettings::default()
            },
            IoConfig::default(),
        )
        .unwrap();

        let filtered = filter.filter(notification);
        assert_eq!(filtered.is_empty(), !expected);
    }

    #[rstest]
    fn test_transaction_filter_includes_meta_for_exact_entry(
        mut notification: TransactionNotification,
    ) {
        notification.entry = "entry/$meta".to_string();

        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                entries: vec!["entry".to_string()],
                ..ReplicationSettings::default()
            },
            IoConfig::default(),
        )
        .unwrap();

        assert_eq!(filter.filter(notification).len(), 1);
    }

    #[rstest]
    #[case("a/x/$meta", vec ! ["/a/*".to_string()], true)]
    #[case("a/private/$meta", vec ! ["/a/**".to_string(), "!/a/private".to_string()], false)]
    fn test_transaction_filter_entries_match_meta_parent(
        #[case] entry: String,
        #[case] entries: Vec<String>,
        #[case] expected: bool,
        mut notification: TransactionNotification,
    ) {
        notification.entry = entry;

        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                entries,
                ..ReplicationSettings::default()
            },
            IoConfig::default(),
        )
        .unwrap();

        assert_eq!(filter.filter(notification).is_empty(), !expected);
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
            IoConfig::default(),
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
            IoConfig::default(),
        )
        .unwrap();

        assert_eq!(filter.filter(notification.clone()).len(), 1);
        assert_eq!(filter.filter(notification.clone()).len(), 0);
        assert_eq!(filter.filter(notification).len(), 1);
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
            IoConfig::default(),
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
            IoConfig::default(),
        )
        .unwrap();

        assert_eq!(filter.filter(notification).len(), 0);
    }

    #[rstest]
    fn test_transaction_filter_when_missing_label_skips_record(
        notification: TransactionNotification,
    ) {
        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&NOT_EXIST", "y"]})),
                ..ReplicationSettings::default()
            },
            IoConfig::default(),
        )
        .unwrap();

        assert!(
            filter.filter(notification).is_empty(),
            "missing labels should not replicate the record"
        );
    }

    #[rstest]
    fn test_transaction_filter_skips_all_filters_for_system_entries(
        mut notification: TransactionNotification,
    ) {
        notification.entry = "entry/$meta".to_string();

        let mut filter = TransactionFilter::try_new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                entries: vec!["entry".to_string()],
                exclude: HashMap::from([("x".to_string(), "y".to_string())]),
                each_n: Some(100),
                when: Some(serde_json::json!({"$eq": ["&NOT_EXIST", "y"]})),
                ..ReplicationSettings::default()
            },
            IoConfig::default(),
        )
        .unwrap();

        assert_eq!(filter.filter(notification).len(), 1);
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
            IoConfig::default(),
        )
        .unwrap();

        assert!(
            filter.filter(notification).is_empty(),
            "interrupted filters should not replicate the record"
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
            IoConfig::default(),
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
