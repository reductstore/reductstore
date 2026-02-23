// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::entry::Entry;
use crate::storage::proto::Record;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{unprocessable_entity, Labels};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

pub(crate) enum WriteDisposition {
    Write,
}

pub(crate) enum UpdateDisposition {
    Persist(Record),
    Removed(Labels),
}

#[async_trait]
pub(crate) trait SystemEntryBehavior {
    fn is_system(&self) -> bool;

    async fn prepare_write(
        &self,
        entry: &Entry,
        labels: &Labels,
    ) -> Result<WriteDisposition, ReductError>;

    async fn apply_update(
        &self,
        entry: Arc<Entry>,
        time: u64,
        record: Record,
        update: Labels,
        remove: HashSet<String>,
    ) -> Result<UpdateDisposition, ReductError>;
}

pub(crate) struct RegularEntryBehavior;

#[async_trait]
impl SystemEntryBehavior for RegularEntryBehavior {
    fn is_system(&self) -> bool {
        false
    }

    async fn prepare_write(
        &self,
        _entry: &Entry,
        _labels: &Labels,
    ) -> Result<WriteDisposition, ReductError> {
        Ok(WriteDisposition::Write)
    }

    async fn apply_update(
        &self,
        _entry: Arc<Entry>,
        _time: u64,
        record: Record,
        update: Labels,
        remove: HashSet<String>,
    ) -> Result<UpdateDisposition, ReductError> {
        Ok(UpdateDisposition::Persist(Entry::update_single_label(
            record, update, remove,
        )))
    }
}

pub(crate) struct MetaEntryBehavior;

#[async_trait]
impl SystemEntryBehavior for MetaEntryBehavior {
    fn is_system(&self) -> bool {
        true
    }

    async fn prepare_write(
        &self,
        entry: &Entry,
        labels: &Labels,
    ) -> Result<WriteDisposition, ReductError> {
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
        Ok(WriteDisposition::Write)
    }

    async fn apply_update(
        &self,
        entry: Arc<Entry>,
        time: u64,
        record: Record,
        update: Labels,
        remove: HashSet<String>,
    ) -> Result<UpdateDisposition, ReductError> {
        let updated = Entry::update_single_label(record, update, remove);
        let labels: Labels = updated
            .labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect();

        if labels.get("remove").is_some_and(|value| value == "true") {
            let errors = entry.clone().remove_records(vec![time]).await?;
            if let Some(err) = errors.get(&time) {
                return Err(err.clone());
            }
            Ok(UpdateDisposition::Removed(labels))
        } else {
            Ok(UpdateDisposition::Persist(updated))
        }
    }
}

pub(crate) fn strategy_for_entry(entry_name: &str) -> Box<dyn SystemEntryBehavior + Send + Sync> {
    if is_system_meta_entry(entry_name) {
        Box::new(MetaEntryBehavior)
    } else {
        Box::new(RegularEntryBehavior)
    }
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
    async fn regular_behavior_is_not_system() {
        assert!(!RegularEntryBehavior.is_system());
    }

    #[tokio::test]
    async fn meta_behavior_is_system() {
        assert!(MetaEntryBehavior.is_system());
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
