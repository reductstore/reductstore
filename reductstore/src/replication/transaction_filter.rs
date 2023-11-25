// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::TransactionNotification;
use reduct_base::Labels;

/// Filter for transaction notifications.
pub(super) struct TransactionFilter {
    bucket: String,
    entries: Vec<String>,
    include: Labels,
    exclude: Labels,
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
    pub(super) fn new(
        bucket: String,
        entries: Vec<String>,
        include: Labels,
        exclude: Labels,
    ) -> Self {
        Self {
            bucket,
            entries,
            include,
            exclude,
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
    pub(super) fn filter(&self, notification: &TransactionNotification) -> bool {
        if notification.bucket != self.bucket {
            return false;
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
                return false;
            }
        }

        // filter out notifications that match does not match the include labels (all must match)
        if !self.include.is_empty() {
            let found = self.include.iter().all(|(key, value)| {
                notification
                    .labels
                    .iter()
                    .any(|label| label.0 == key && label.1 == value)
            });

            if !found {
                return false;
            }
        }

        // filter out notifications that match the exclude labels (any must match)
        if !self.exclude.is_empty() {
            let found = self.exclude.iter().any(|(key, value)| {
                notification
                    .labels
                    .iter()
                    .any(|label| label.0 == key && label.1 == value)
            });

            if found {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::replication::Transaction;
    use rstest::*;

    #[rstest]
    fn test_transaction_filter(notification: TransactionNotification) {
        let filter =
            TransactionFilter::new("bucket".to_string(), vec![], Labels::new(), Labels::new());
        assert!(filter.filter(&notification));
    }

    #[rstest]
    fn test_transaction_filter_bucket(notification: TransactionNotification) {
        let filter =
            TransactionFilter::new("other".to_string(), vec![], Labels::new(), Labels::new());
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
        let filter =
            TransactionFilter::new("bucket".to_string(), entries, Labels::new(), Labels::new());
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
        let filter = TransactionFilter::new(
            "bucket".to_string(),
            vec![],
            Labels::from_iter(include),
            Labels::new(),
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
        let filter = TransactionFilter::new(
            "bucket".to_string(),
            vec![],
            Labels::new(),
            Labels::from_iter(exclude),
        );
        assert_eq!(filter.filter(&notification), expected);
    }

    #[fixture]
    fn notification() -> TransactionNotification {
        let labels = HashMap::from_iter(vec![
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
