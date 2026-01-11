// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::bucket::Bucket;
use crate::storage::entry::update_labels::UpdateLabels;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::collections::{BTreeMap, HashSet};

#[derive(Clone)]
pub(crate) struct UpdateLabelsMulti {
    pub entry_name: String,
    pub time: u64,
    pub update: Labels,
    pub remove: HashSet<String>,
}

type UpdateResult = BTreeMap<String, BTreeMap<u64, Result<Labels, ReductError>>>;

impl Bucket {
    /// Update labels for multiple records across entries.
    ///
    /// The method updates labels for multiple records. The records are identified by their timestamps
    /// and batched by the block they belong to
    ///
    /// # Arguments
    ///
    /// * `updates` - A vector of `UpdateLabels` structs that contain the timestamp of the record to update,
    ///
    /// # Returns
    ///
    /// A map of timestamps to the result of the update operation. The result is either a vector of labels
    /// or an error if the record was not found.
    pub async fn update_labels(
        &self,
        updates: Vec<UpdateLabelsMulti>,
    ) -> Result<UpdateResult, ReductError> {
        let mut result: UpdateResult = BTreeMap::new();
        let mut updates_per_entry: BTreeMap<String, Vec<(u64, Labels, HashSet<String>)>> =
            BTreeMap::new();

        for update in updates {
            updates_per_entry
                .entry(update.entry_name.clone())
                .or_default()
                .push((update.time, update.update, update.remove));
        }

        for (entry_name, entry_updates) in updates_per_entry {
            match self.get_entry(&entry_name).await {
                Ok(entry) => {
                    let entry = entry.upgrade()?;
                    let formatted_updates = entry_updates
                        .into_iter()
                        .map(|(time, update, remove)| UpdateLabels {
                            time,
                            update,
                            remove,
                        })
                        .collect();
                    let entry_results = entry.update_labels(formatted_updates).await?;
                    result.insert(entry_name, entry_results);
                }
                Err(e) => {
                    let mut entry_result = BTreeMap::new();
                    for (time, _, _) in entry_updates {
                        entry_result.insert(time, Err(e.clone()));
                    }
                    result.insert(entry_name, entry_result);
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bucket::tests::{bucket, write};
    use crate::storage::entry::Entry;
    use bytes::Bytes;
    use reduct_base::io::ReadRecord;
    use reduct_base::not_found;
    use rstest::rstest;
    use std::sync::Arc;

    async fn write_with_labels(entry: &Arc<Entry>, time: u64, labels: Labels) {
        let mut sender = entry
            .begin_write(time, 1, "text/plain".to_string(), labels)
            .await
            .unwrap();
        sender
            .send(Ok(Some(Bytes::from_static(b"x"))))
            .await
            .unwrap();
        sender.send(Ok(None)).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn updates_labels_across_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let entry1 = bucket
            .get_or_create_entry("entry-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let entry2 = bucket
            .get_or_create_entry("entry-2")
            .await
            .unwrap()
            .upgrade()
            .unwrap();

        write_with_labels(
            &entry1,
            1,
            Labels::from_iter(vec![
                ("keep".to_string(), "v1".to_string()),
                ("drop".to_string(), "tmp".to_string()),
            ]),
        )
        .await;
        write_with_labels(
            &entry2,
            2,
            Labels::from_iter(vec![("foo".to_string(), "bar".to_string())]),
        )
        .await;

        let result = bucket
            .update_labels(vec![
                UpdateLabelsMulti {
                    entry_name: "entry-1".into(),
                    time: 1,
                    update: Labels::from_iter(vec![("keep".into(), "v2".into())]),
                    remove: HashSet::from_iter(vec!["drop".to_string()]),
                },
                UpdateLabelsMulti {
                    entry_name: "entry-2".into(),
                    time: 2,
                    update: Labels::from_iter(vec![("new".into(), "v".into())]),
                    remove: HashSet::new(),
                },
            ])
            .await
            .unwrap();

        let entry1_labels = result
            .get("entry-1")
            .unwrap()
            .get(&1)
            .unwrap()
            .as_ref()
            .unwrap();
        assert_eq!(
            entry1_labels,
            &Labels::from_iter(vec![("keep".into(), "v2".into())])
        );

        let entry2_labels = result
            .get("entry-2")
            .unwrap()
            .get(&2)
            .unwrap()
            .as_ref()
            .unwrap();
        assert_eq!(
            entry2_labels,
            &Labels::from_iter(vec![
                ("foo".into(), "bar".into()),
                ("new".into(), "v".into())
            ])
        );

        // Verify stored records were updated.
        let stored1 = entry1.begin_read(1).await.unwrap();
        assert_eq!(stored1.meta().labels(), entry1_labels);
        let stored2 = entry2.begin_read(2).await.unwrap();
        assert_eq!(stored2.meta().labels(), entry2_labels);
    }

    #[rstest]
    #[tokio::test]
    async fn returns_error_for_missing_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "present", 1, b"a").await.unwrap();

        let result = bucket
            .update_labels(vec![
                UpdateLabelsMulti {
                    entry_name: "present".into(),
                    time: 1,
                    update: Labels::from_iter(vec![("a".into(), "b".into())]),
                    remove: HashSet::new(),
                },
                UpdateLabelsMulti {
                    entry_name: "missing".into(),
                    time: 2,
                    update: Labels::new(),
                    remove: HashSet::new(),
                },
            ])
            .await
            .unwrap();

        assert!(result
            .get("present")
            .unwrap()
            .get(&1)
            .unwrap()
            .as_ref()
            .unwrap()
            .contains_key("a"));

        let err = result
            .get("missing")
            .unwrap()
            .get(&2)
            .unwrap()
            .as_ref()
            .err()
            .unwrap();
        assert_eq!(
            err,
            &not_found!("Entry 'missing' not found in bucket 'test'")
        );
    }
}
