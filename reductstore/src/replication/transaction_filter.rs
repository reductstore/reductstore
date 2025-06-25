// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use log::warn;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::msg::replication_api::ReplicationSettings;

use crate::replication::TransactionNotification;
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::{
    apply_filters_recursively, EachNFilter, EachSecondFilter, ExcludeLabelFilter,
    IncludeLabelFilter, RecordFilter, WhenFilter,
};

type Filter = Box<dyn RecordFilter<TransactionNotification> + Send + Sync>;
/// Filter for transaction notifications.
pub(super) struct TransactionFilter {
    bucket: String,
    entries: Vec<String>,
    query_filters: Vec<Filter>,
}

impl Into<RecordMeta> for TransactionNotification {
    fn into(self) -> RecordMeta {
        RecordMeta::builder()
            .timestamp(self.event.into_timestamp())
            .state(0)
            .labels(self.labels)
            .build()
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
    pub(super) fn new(name: &str, settings: ReplicationSettings) -> Self {
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
            match Parser::new().parse(&when) {
                Ok(condition) => {
                    query_filters.push(Box::new(WhenFilter::new(condition, true)));
                }
                Err(err) => warn!(
                    "Error parsing when condition in {} replication task: {}",
                    name, err
                ),
            }
        }

        Self {
            bucket: settings.src_bucket,
            entries: settings.entries,
            query_filters,
        }
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
    use reduct_base::Labels;
    use rstest::*;

    use crate::replication::Transaction;

    use super::*;

    #[rstest]
    fn test_transaction_filter(notification: TransactionNotification) {
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                ..ReplicationSettings::default()
            },
        );
        assert!(filter.filter(&notification));
    }

    #[rstest]
    fn test_transaction_filter_bucket(notification: TransactionNotification) {
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "other".to_string(),
                ..ReplicationSettings::default()
            },
        );
        assert!(!filter.filter(&notification));
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
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                entries,
                ..ReplicationSettings::default()
            },
        );
        assert_eq!(filter.filter(&notification), expected);
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
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                include: Labels::from_iter(include),
                ..ReplicationSettings::default()
            },
        );
        assert_eq!(filter.filter(&notification), expected);
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
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                exclude: Labels::from_iter(exclude),
                ..ReplicationSettings::default()
            },
        );
        assert_eq!(filter.filter(&notification), expected);
    }

    #[rstest]
    fn test_transaction_filter_each_n(notification: TransactionNotification) {
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                each_n: Some(2),
                ..ReplicationSettings::default()
            },
        );

        assert!(filter.filter(&notification));
        assert!(!filter.filter(&notification));
        assert!(filter.filter(&notification));
    }

    #[rstest]
    fn test_transaction_filter_each_s(mut notification: TransactionNotification) {
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                each_s: Some(1.0),
                ..ReplicationSettings::default()
            },
        );

        assert!(filter.filter(&notification));
        notification.event = Transaction::WriteRecord(1);
        assert!(!filter.filter(&notification));
        notification.event = Transaction::WriteRecord(2);
        assert!(!filter.filter(&notification));
        notification.event = Transaction::WriteRecord(1000_002);
        assert!(filter.filter(&notification));
    }

    #[rstest]
    fn test_transaction_filter_when(notification: TransactionNotification) {
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&x", "y"]})),
                ..ReplicationSettings::default()
            },
        );

        assert!(filter.filter(&notification));
        filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&x", "z"]})),
                ..ReplicationSettings::default()
            },
        );
        assert!(!filter.filter(&notification));
    }

    #[rstest]
    fn test_transaction_filter_when_non_strict(notification: TransactionNotification) {
        let mut filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$eq": ["&NOT_EXIST", "y"]})),
                ..ReplicationSettings::default()
            },
        );

        assert!(
            !filter.filter(&notification),
            "label doesn't exist but we consider it as false"
        );
    }

    #[rstest]
    fn test_transaction_filter_invalid_when() {
        let filter = TransactionFilter::new(
            "test",
            ReplicationSettings {
                src_bucket: "bucket".to_string(),
                when: Some(serde_json::json!({"$UNKNOWN_OP": ["&x", "y", "z"]})),
                ..ReplicationSettings::default()
            },
        );

        assert!(filter.query_filters.is_empty());
    }

    mod filter_point {

        use super::*;

        #[rstest]
        fn test_filter_point(notification: TransactionNotification) {
            let meta: RecordMeta = notification.clone().into();
            assert_eq!(meta.timestamp(), 0);
            assert_eq!(meta.labels(), &notification.labels);
            assert_eq!(meta.state(), 0);
        }
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
            labels,
            event: Transaction::WriteRecord(0),
        }
    }
}
