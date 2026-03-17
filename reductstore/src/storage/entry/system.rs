// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::entry::Entry;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{unprocessable_entity, Labels};
use std::collections::HashMap;

/// Reserved segment for system metadata entries.
///
/// Rules:
/// - Metadata for entry `<entry>` is stored in `<entry>/$meta`.
/// - `$meta` entries are system entries and cannot be removed directly.
/// - `$meta` entries are included in storage usage/size accounting.
/// - `$meta` entries are excluded from user-facing entry counts/lists.
/// - FIFO quota eviction must ignore `$meta` entries as removal candidates.
/// - Every `$meta` record must include label `key`.
/// - `$meta` behaves as upsert-by-key: writing a record with `key=K` removes existing `$meta`
///   records with the same key before adding the new one.
/// - Updating a `$meta` record with label `remove=true` creates a tombstone (label-only update)
///   and does not physically delete the record.
pub(crate) const META_ENTRY_SEGMENT: &str = "$meta";

/// Max block size for system metadata entries.
pub(crate) const META_ENTRY_MAX_BLOCK_SIZE: u64 = 128 * 1024;

pub(crate) fn is_system_meta_entry(entry_name: &str) -> bool {
    entry_name == META_ENTRY_SEGMENT || entry_name.ends_with(&format!("/{}", META_ENTRY_SEGMENT))
}

pub(crate) fn meta_entry_parent(entry_name: &str) -> Option<&str> {
    entry_name
        .strip_suffix(&format!("/{}", META_ENTRY_SEGMENT))
        .filter(|base| !base.is_empty())
}

pub(crate) fn meta_entry_name(entry_name: &str) -> String {
    format!("{entry_name}/{META_ENTRY_SEGMENT}")
}

#[async_trait]
pub(crate) trait SystemEntryBehavior {
    fn is_visible_in_bucket_info(&self) -> bool {
        true
    }

    fn is_eligible_for_fifo_eviction(&self) -> bool {
        true
    }

    fn is_removable_by_query(&self) -> bool {
        true
    }

    fn is_queryable_by_wildcard(&self) -> bool {
        true
    }

    fn validate_remove_records(&self, _entry_name: &str) -> Result<(), ReductError> {
        Ok(())
    }

    fn validate_remove_entry(&self, _entry_name: &str) -> Result<(), ReductError> {
        Ok(())
    }

    async fn prepare_write(&self, entry: &Entry, labels: &Labels) -> Result<(), ReductError>;

    fn apply_default_query_filters(&self, query: &mut QueryEntry);
}

pub(crate) struct RegularEntryBehavior;

#[async_trait]
impl SystemEntryBehavior for RegularEntryBehavior {
    async fn prepare_write(&self, _entry: &Entry, _labels: &Labels) -> Result<(), ReductError> {
        Ok(())
    }

    fn apply_default_query_filters(&self, _query: &mut QueryEntry) {}
}

pub(crate) struct MetaEntryBehavior;

#[async_trait]
impl SystemEntryBehavior for MetaEntryBehavior {
    async fn prepare_write(&self, entry: &Entry, labels: &Labels) -> Result<(), ReductError> {
        let key = labels.get("key").ok_or_else(|| {
            unprocessable_entity!(
                "System entry '{}' records must contain label 'key'",
                entry.name()
            )
        })?;

        if labels.get("remove").is_some_and(|value| value == "true") {
            return Err(unprocessable_entity!(
                "System entry '{}' does not support writing records with label 'remove=true'; use record update",
                entry.name()
            ));
        }

        let _ = entry
            .query_remove_records(QueryEntry {
                start: Some(0),
                stop: Some(u64::MAX),
                include: Some(HashMap::from([("key".to_string(), key.to_string())])),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    fn apply_default_query_filters(&self, query: &mut QueryEntry) {
        if query
            .include
            .as_ref()
            .is_some_and(|include| include.contains_key("remove"))
        {
            return;
        }

        let exclude = query.exclude.get_or_insert_with(HashMap::new);
        exclude
            .entry("remove".to_string())
            .or_insert_with(|| "true".to_string());
    }

    fn validate_remove_records(&self, entry_name: &str) -> Result<(), ReductError> {
        Err(unprocessable_entity!(
            "Can't delete records from system entry '{}'; use label update with remove=true",
            entry_name
        ))
    }

    fn validate_remove_entry(&self, entry_name: &str) -> Result<(), ReductError> {
        Err(unprocessable_entity!(
            "Can't delete system entry '{}'; remove the parent entry instead",
            entry_name
        ))
    }

    fn is_visible_in_bucket_info(&self) -> bool {
        false
    }

    fn is_eligible_for_fifo_eviction(&self) -> bool {
        false
    }

    fn is_removable_by_query(&self) -> bool {
        false
    }

    fn is_queryable_by_wildcard(&self) -> bool {
        false
    }
}

pub(crate) fn strategy_for_entry(entry_name: &str) -> Box<dyn SystemEntryBehavior + Send + Sync> {
    if is_system_meta_entry(entry_name) {
        Box::new(MetaEntryBehavior)
    } else {
        Box::new(RegularEntryBehavior)
    }
}

pub(crate) fn validate_remove_records(entry_name: &str) -> Result<(), ReductError> {
    strategy_for_entry(entry_name).validate_remove_records(entry_name)
}

pub(crate) fn validate_remove_entry(entry_name: &str) -> Result<(), ReductError> {
    strategy_for_entry(entry_name).validate_remove_entry(entry_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::entry::{Entry, EntrySettings};
    use bytes::Bytes;
    use reduct_base::error::ReductError;
    use serial_test::serial;
    use tempfile::tempdir;

    async fn build_entry(name: &str) -> Entry {
        let path = tempdir().unwrap().keep();
        Entry::try_build(
            name,
            path,
            EntrySettings {
                max_block_size: 1024 * 1024,
                max_block_records: 1024,
            },
            Cfg::default().into(),
        )
        .await
        .unwrap()
    }

    async fn write_record(entry: &Entry, ts: u64, labels: Labels) {
        let mut writer = entry
            .begin_write(ts, 1, "application/octet-stream".to_string(), labels)
            .await
            .unwrap();
        writer
            .send(Ok(Some(Bytes::from_static(b"x"))))
            .await
            .unwrap();
        writer.send(Ok(None)).await.unwrap();
    }

    #[tokio::test]
    async fn regular_behavior_allows_delete() {
        assert!(RegularEntryBehavior
            .validate_remove_records("entry-1")
            .is_ok());
        assert!(RegularEntryBehavior
            .validate_remove_entry("entry-1")
            .is_ok());
        assert!(RegularEntryBehavior.is_visible_in_bucket_info());
        assert!(RegularEntryBehavior.is_eligible_for_fifo_eviction());
        assert!(RegularEntryBehavior.is_removable_by_query());
        assert!(RegularEntryBehavior.is_queryable_by_wildcard());
    }

    #[tokio::test]
    async fn meta_behavior_blocks_delete() {
        let err = MetaEntryBehavior
            .validate_remove_records("entry-1/$meta")
            .err()
            .unwrap();
        assert_eq!(
            err,
            ReductError::unprocessable_entity(
                "Can't delete records from system entry 'entry-1/$meta'; use label update with remove=true"
            )
        );

        let err = MetaEntryBehavior
            .validate_remove_entry("entry-1/$meta")
            .err()
            .unwrap();
        assert_eq!(
            err,
            ReductError::unprocessable_entity(
                "Can't delete system entry 'entry-1/$meta'; remove the parent entry instead"
            )
        );
        assert!(!MetaEntryBehavior.is_visible_in_bucket_info());
        assert!(!MetaEntryBehavior.is_eligible_for_fifo_eviction());
        assert!(!MetaEntryBehavior.is_removable_by_query());
        assert!(!MetaEntryBehavior.is_queryable_by_wildcard());
    }

    #[tokio::test]
    async fn meta_behavior_adds_default_remove_filter() {
        let mut query = QueryEntry::default();
        MetaEntryBehavior.apply_default_query_filters(&mut query);
        assert_eq!(
            query.exclude.unwrap().get("remove"),
            Some(&"true".to_string())
        );
    }

    #[tokio::test]
    async fn meta_behavior_does_not_override_explicit_remove_include() {
        let mut query = QueryEntry {
            include: Some(HashMap::from([("remove".to_string(), "true".to_string())])),
            ..Default::default()
        };
        MetaEntryBehavior.apply_default_query_filters(&mut query);
        assert!(query.exclude.is_none());
    }

    #[tokio::test]
    async fn meta_prepare_write_requires_key_label() {
        let entry = build_entry("entry/$meta").await;
        let err = MetaEntryBehavior
            .prepare_write(&entry, &Labels::new())
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            ReductError::unprocessable_entity(
                "System entry 'entry/$meta' records must contain label 'key'"
            )
        );
    }

    #[tokio::test]
    async fn meta_prepare_write_rejects_remove_true() {
        let entry = build_entry("entry/$meta").await;
        let err = MetaEntryBehavior
            .prepare_write(
                &entry,
                &Labels::from_iter([
                    ("key".to_string(), "plugin".to_string()),
                    ("remove".to_string(), "true".to_string()),
                ]),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            ReductError::unprocessable_entity(
                "System entry 'entry/$meta' does not support writing records with label 'remove=true'; use record update"
            )
        );
    }

    #[tokio::test]
    #[serial]
    async fn meta_prepare_write_removes_existing_records_with_same_key() {
        let entry = build_entry("entry/$meta").await;
        write_record(
            &entry,
            1,
            Labels::from_iter([("key".to_string(), "schema".to_string())]),
        )
        .await;
        assert!(entry.begin_read(1).await.is_ok());

        MetaEntryBehavior
            .prepare_write(
                &entry,
                &Labels::from_iter([("key".to_string(), "schema".to_string())]),
            )
            .await
            .unwrap();

        assert!(entry.begin_read(1).await.is_err());
    }
}
