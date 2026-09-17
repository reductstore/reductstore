// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::bucket::Bucket;
use crate::storage::entry::{entry_matches_pattern, RecordQueryStats};
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryEntry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

impl Bucket {
    pub(super) fn requested_entries(entries: &Option<Vec<String>>) -> Option<Vec<String>> {
        match entries {
            Some(entries) if !entries.is_empty() => Some(entries.clone()),
            _ => None,
        }
    }

    /// Check whether an entry name is selected by a set of glob-like patterns.
    ///
    /// Patterns share the semantics of query and replication entry filters:
    /// exact names, legacy prefix wildcards (`cam-*`), single-segment `*`,
    /// recursive `**`, and `!` exclusions applied after the includes.
    pub(super) fn entry_matches_patterns(entry: &str, patterns: &[String]) -> bool {
        let include_patterns: Vec<&str> = patterns
            .iter()
            .filter(|pattern| !(pattern.starts_with('!') && pattern.len() > 1))
            .map(|pattern| pattern.as_str())
            .collect();
        let exclude_patterns: Vec<&str> = patterns
            .iter()
            .filter_map(|pattern| pattern.strip_prefix('!'))
            .filter(|pattern| !pattern.is_empty())
            .collect();

        let included = include_patterns.is_empty()
            || include_patterns
                .iter()
                .any(|pattern| entry_matches_pattern(entry, pattern));

        included
            && !exclude_patterns
                .iter()
                .any(|pattern| entry_matches_pattern(entry, pattern))
    }

    pub(super) fn is_requested_entry(
        entry_name: &str,
        requested_entries: &Option<Vec<String>>,
    ) -> bool {
        requested_entries
            .as_ref()
            .map(|patterns| Self::entry_matches_patterns(entry_name, patterns))
            .unwrap_or(true)
    }

    /// Remove records from the bucket
    ///
    /// # Arguments
    ///
    /// * `record_ids` - A map where the key is the entry name and the value is a vector of record IDs to remove.
    pub async fn remove_records(
        self: Arc<Self>,
        record_ids: HashMap<String, Vec<u64>>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        self.ensure_not_deleting().await?;
        let mut results = BTreeMap::new();

        for (entry_name, ids) in record_ids {
            match self.get_entry(&entry_name).await {
                Ok(entry) => {
                    let entry = entry.upgrade()?;
                    let entry_results = entry.remove_records(ids).await?;
                    results.extend(entry_results);
                }
                Err(e) => {
                    for id in ids {
                        results.insert(id, e.clone());
                    }
                }
            }
        }

        Ok(results)
    }

    /// Query and remove multiple records over a range of timestamps.
    ///
    /// # Arguments
    ///
    /// * `options` - The query options.
    ///
    /// # Returns
    /// The number of records removed.
    pub async fn query_remove_records(
        self: Arc<Self>,
        options: QueryEntry,
    ) -> Result<u64, ReductError> {
        Ok(self.query_remove_records_with_stats(options).await?.records)
    }

    pub(crate) async fn query_remove_records_with_stats(
        self: Arc<Self>,
        options: QueryEntry,
    ) -> Result<RecordQueryStats, ReductError> {
        self.ensure_not_deleting().await?;
        let entries = self.entries.read().await?.clone();
        let requested_entries = Self::requested_entries(&options.entries);
        let mut total_removed = RecordQueryStats::default();

        for (entry_name, entry) in entries {
            if !Self::is_requested_entry(&entry_name, &requested_entries) {
                continue;
            }

            if !entry.is_removable_by_query() {
                continue;
            }

            entry.ensure_not_deleting().await?;
            let removed = entry
                .query_remove_records_with_stats(options.clone())
                .await?;
            total_removed.records += removed.records;
            total_removed.blocks += removed.blocks;
        }

        Ok(total_removed)
    }

    /// Query and count multiple records over a range of timestamps.
    ///
    /// # Arguments
    ///
    /// * `options` - The query options.
    ///
    /// # Returns
    /// The number of records matched by the query.
    #[allow(dead_code)]
    pub async fn query_count_records(
        self: Arc<Self>,
        options: QueryEntry,
    ) -> Result<u64, ReductError> {
        Ok(self.query_count_records_with_stats(options).await?.records)
    }

    pub(crate) async fn query_count_records_with_stats(
        self: Arc<Self>,
        options: QueryEntry,
    ) -> Result<RecordQueryStats, ReductError> {
        let entries = self.entries.read().await?.clone();
        let requested_entries = Self::requested_entries(&options.entries);
        let mut total_counted = RecordQueryStats::default();

        for (entry_name, entry) in entries {
            if !Self::is_requested_entry(&entry_name, &requested_entries) {
                continue;
            }

            if !entry.is_removable_by_query() {
                continue;
            }

            let counted = entry
                .query_count_records_with_stats(options.clone())
                .await?;
            total_counted.records += counted.records;
            total_counted.blocks += counted.blocks;
        }

        Ok(total_counted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bucket::tests::{bucket, write, write_meta};
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use reduct_base::{conflict, not_found};
    use rstest::rstest;
    use std::collections::HashMap;

    #[rstest]
    #[case("entry-1", vec!["entry-1".to_string()], true)]
    #[case("entry-1", vec!["entry-*".to_string()], true)]
    #[case("other", vec!["entry-*".to_string()], false)]
    #[case("a/x/b", vec!["/a/*/b".to_string()], true)]
    #[case("a/x/d/b", vec!["/a/*/b".to_string()], false)]
    #[case("a/x/b", vec!["/a/**/b".to_string()], true)]
    #[case("a/x/d/b", vec!["/a/**/b".to_string()], true)]
    #[case("public", vec!["!secret".to_string()], true)]
    #[case("secret", vec!["!secret".to_string()], false)]
    #[case("a/private/x", vec!["/a/**".to_string(), "!/a/private/**".to_string()], false)]
    #[case("a/public/x", vec!["/a/**".to_string(), "!/a/private/**".to_string()], true)]
    fn entry_matches_patterns_supports_globs_and_exclusions(
        #[case] entry: &str,
        #[case] patterns: Vec<String>,
        #[case] expected: bool,
    ) {
        assert_eq!(Bucket::entry_matches_patterns(entry, &patterns), expected);
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_supports_recursive_and_exclusion(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "a/public/b", 1, b"pub").await.unwrap();
        write(&bucket, "a/private/b", 1, b"priv").await.unwrap();
        write(&bucket, "other", 1, b"other").await.unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["/a/**".into(), "!/a/private/**".into()]),
            start: Some(1),
            stop: Some(2),
            ..Default::default()
        };

        let removed = bucket.clone().query_remove_records(request).await.unwrap();
        assert_eq!(removed, 1);

        assert!(bucket.begin_read("a/public/b", 1).await.is_err());
        assert!(bucket.begin_read("a/private/b", 1).await.is_ok());
        assert!(bucket.begin_read("other", 1).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn removes_records_from_multiple_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-b", 2, b"b1").await.unwrap();
        write(&bucket, "entry-b", 3, b"b2").await.unwrap();

        let errors = bucket
            .clone()
            .remove_records(HashMap::from([
                ("entry-a".to_string(), vec![1]),
                ("entry-b".to_string(), vec![2, 4]),
                ("missing".to_string(), vec![5]),
            ]))
            .await
            .unwrap();

        assert_eq!(errors.len(), 2);
        assert_eq!(
            errors[&4],
            not_found!("Record 4 not found in entry test/entry-b")
        );
        assert_eq!(
            errors[&5],
            not_found!("Entry 'missing' not found in bucket 'test'")
        );

        assert_eq!(
            bucket.begin_read("entry-a", 1).await.err().unwrap(),
            not_found!("Record 1 not found in entry test/entry-a")
        );
        assert_eq!(
            bucket.begin_read("entry-b", 2).await.err().unwrap(),
            not_found!("Record 2 not found in block test/entry-b/2")
        );
        assert!(bucket.begin_read("entry-b", 3).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_filters_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-a", 4, b"a2").await.unwrap();
        write(&bucket, "entry-b", 2, b"b1").await.unwrap();
        write(&bucket, "entry-c", 2, b"c1").await.unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            start: Some(1),
            stop: Some(3),
            ..Default::default()
        };

        let removed = bucket.clone().query_remove_records(request).await.unwrap();
        assert_eq!(removed, 2);

        assert_eq!(
            bucket.begin_read("entry-a", 1).await.err().unwrap(),
            not_found!("Record 1 not found in block test/entry-a/1")
        );
        assert_eq!(
            bucket.begin_read("entry-b", 2).await.err().unwrap(),
            not_found!("Record 2 not found in entry test/entry-b")
        );
        assert!(bucket.begin_read("entry-a", 4).await.is_ok());
        assert!(bucket.begin_read("entry-c", 2).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_returns_conflict_when_target_entry_deleting(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        let entry = bucket
            .get_entry("entry-a")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        entry.mark_deleting().await.unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["entry-a".into()]),
            start: Some(1),
            stop: Some(2),
            ..Default::default()
        };

        let err = bucket
            .clone()
            .query_remove_records(request)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            conflict!("Entry 'entry-a' in bucket 'test' is being deleted")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_supports_wildcards(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-one", 1, b"one-1").await.unwrap();
        write(&bucket, "entry-one", 2, b"one-2").await.unwrap();
        write(&bucket, "entry-two", 1, b"two-1").await.unwrap();
        write(&bucket, "other", 1, b"other-1").await.unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["entry-*".into()]),
            start: Some(1),
            stop: Some(2),
            ..Default::default()
        };

        let removed = bucket.clone().query_remove_records(request).await.unwrap();
        assert_eq!(removed, 2);

        assert_eq!(
            bucket.begin_read("entry-one", 1).await.err().unwrap(),
            not_found!("Record 1 not found in block test/entry-one/1")
        );
        assert_eq!(
            bucket.begin_read("entry-two", 1).await.err().unwrap(),
            not_found!("Record 1 not found in entry test/entry-two")
        );
        assert!(bucket.begin_read("entry-one", 2).await.is_ok());
        assert!(bucket.begin_read("other", 1).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_wildcard_excludes_meta_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-one", 1, b"one-1").await.unwrap();
        write_meta(&bucket, "entry-one/$meta", 1, b"meta-1")
            .await
            .unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["entry-one*".into()]),
            start: Some(1),
            stop: Some(2),
            ..Default::default()
        };

        let removed = bucket.clone().query_remove_records(request).await.unwrap();
        assert_eq!(removed, 1);

        assert!(bucket.begin_read("entry-one", 1).await.is_err());
        assert!(bucket.begin_read("entry-one/$meta", 1).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_supports_all_entries_wildcard(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-b", 1, b"b1").await.unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["*".into()]),
            start: Some(1),
            stop: Some(2),
            ..Default::default()
        };

        let removed = bucket.clone().query_remove_records(request).await.unwrap();
        assert_eq!(removed, 2);

        assert_eq!(
            bucket.begin_read("entry-a", 1).await.err().unwrap(),
            not_found!("Record 1 not found in entry test/entry-a")
        );
        assert_eq!(
            bucket.begin_read("entry-b", 1).await.err().unwrap(),
            not_found!("Record 1 not found in entry test/entry-b")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn query_count_records_filters_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-a", 4, b"a2").await.unwrap();
        write(&bucket, "entry-b", 2, b"b1").await.unwrap();
        write(&bucket, "entry-c", 2, b"c1").await.unwrap();

        let request = QueryEntry {
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            start: Some(1),
            stop: Some(3),
            ..Default::default()
        };

        let counted = bucket.clone().query_count_records(request).await.unwrap();
        assert_eq!(counted, 2);

        assert!(bucket.begin_read("entry-a", 1).await.is_ok());
        assert!(bucket.begin_read("entry-b", 2).await.is_ok());
        assert!(bucket.begin_read("entry-a", 4).await.is_ok());
        assert!(bucket.begin_read("entry-c", 2).await.is_ok());
    }
}
