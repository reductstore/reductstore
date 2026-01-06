// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::thread_pool::{spawn, TaskHandle};
use crate::storage::entry::Entry;
use crate::storage::proto::record::Label;
use crate::storage::proto::Record;
use reduct_base::error::ReductError;
use reduct_base::{not_found, Labels};
use reduct_macros::task;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

/// A struct that contains the timestamp of the record to update, the labels to update and the labels to remove.
pub(crate) struct UpdateLabels {
    pub time: u64,
    pub update: Labels,
    pub remove: HashSet<String>,
}

type UpdateResult = BTreeMap<u64, Result<Labels, ReductError>>;

impl Entry {
    /// Update labels for multiple records.
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
    #[task("update labels")]
    pub fn update_labels(
        self: Arc<Self>,
        updates: Vec<UpdateLabels>,
    ) -> Result<UpdateResult, ReductError> {
        let mut result = UpdateResult::new();
        let mut records_per_block = BTreeMap::new();

        {
            let mut bm = self.block_manager.write()?;
            for UpdateLabels {
                time,
                update,
                remove,
            } in updates
            {
                // Find the block that contains the record
                // TODO: Try to avoid the lookup for each record
                match bm.find_block(time) {
                    Ok(block_ref) => {
                        let block = block_ref.read()?;
                        if let Some(record) = block.get_record(time) {
                            let record = Self::update_single_label(record.clone(), update, remove);
                            records_per_block
                                .entry(block.block_id())
                                .or_insert_with(Vec::new)
                                .push(record.clone());
                            result.insert(
                                time,
                                Ok(record
                                    .labels
                                    .iter()
                                    .map(|label| (label.name.clone(), label.value.clone()))
                                    .collect()),
                            );
                        } else {
                            result
                                .insert(time, Err(not_found!("No record with timestamp {}", time)));
                        }
                    }
                    Err(err) => {
                        result.insert(time, Err(err));
                    }
                }
            }
        }

        // Update blocks
        let mut handlers = Vec::new();
        for (block_id, records) in records_per_block.into_iter() {
            let local_block_manager = self.block_manager.clone();
            let handler: TaskHandle<Result<(), ReductError>> =
                spawn("update labels in block", move || {
                    let mut bm = local_block_manager.write().unwrap();
                    bm.update_records(block_id, records)?;
                    Ok(())
                });

            handlers.push(handler);
        }

        // Wait for all handlers to finish
        for handler in handlers {
            handler.wait()?;
        }
        Ok(result)
    }

    fn update_single_label(
        mut record: Record,
        mut update: Labels,
        remove: HashSet<String>,
    ) -> Record {
        let mut new_labels = Vec::new();
        for label in &record.labels {
            // remove labels
            if remove.contains(label.name.as_str()) {
                continue;
            }

            // update existing labels or add new labels
            match update.remove(label.name.as_str()) {
                Some(value) => {
                    new_labels.push(Label {
                        name: label.name.clone(),
                        value,
                    });
                }
                None => {
                    new_labels.push(label.clone());
                }
            }
        }

        // add new labels
        for (name, value) in update {
            new_labels.push(Label { name, value });
        }

        record.labels = new_labels;
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::tests::{entry, write_record_with_labels};

    use crate::storage::entry::EntrySettings;
    use reduct_base::io::ReadRecord;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_update_labels(mut entry: Entry) {
        entry.set_settings(EntrySettings {
            max_block_records: 2,
            ..entry.settings()
        });
        write_stub_record(&mut entry, 1).await;
        write_stub_record(&mut entry, 2).await;
        write_stub_record(&mut entry, 3).await;
        let entry = Arc::new(entry);

        // update, remove and add labels
        let result = entry
            .update_labels(vec![
                make_update(0),
                make_update(1),
                make_update(2),
                make_update(3),
                make_update(5),
            ])
            .wait()
            .unwrap();

        // check results
        assert_eq!(result.len(), 5, "result contains entry for each update");
        assert_eq!(
            result[&0].as_ref().err().unwrap(),
            &not_found!("No record with timestamp 0")
        );
        assert_eq!(
            result[&5].as_ref().err().unwrap(),
            &not_found!("No record with timestamp 5")
        );

        let updated_labels = result.get(&1).unwrap().as_ref().unwrap();
        let expected_labels_1 = make_expected_labels(1);
        assert_eq!(updated_labels, &expected_labels_1);

        let updated_labels = result.get(&2).unwrap().as_ref().unwrap();
        let expected_labels_2 = make_expected_labels(2);
        assert_eq!(updated_labels, &expected_labels_2);

        let updated_labels = result.get(&3).unwrap().as_ref().unwrap();
        let expected_labels_3 = make_expected_labels(3);
        assert_eq!(updated_labels, &expected_labels_3);

        // check if the records were updated
        let labels = entry.begin_read(1).wait().unwrap().meta().labels().clone();
        assert_eq!(labels, expected_labels_1);

        let labels = entry.begin_read(2).wait().unwrap().meta().labels().clone();
        assert_eq!(labels, expected_labels_2);

        let labels = entry.begin_read(3).wait().unwrap().meta().labels().clone();
        assert_eq!(labels, expected_labels_3);
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_nothing(mut entry: Entry) {
        write_stub_record(&mut entry, 1).await;
        let entry = Arc::new(entry);
        let result = entry
            .update_labels(vec![UpdateLabels {
                time: 1,
                update: Labels::new(),
                remove: HashSet::new(),
            }])
            .wait()
            .unwrap();

        assert_eq!(result.len(), 1);

        let updated_labels = result.get(&1).unwrap().as_ref().unwrap();
        let expected_labels = vec![
            Label {
                name: "a-1".to_string(),
                value: "x-1".to_string(),
            },
            Label {
                name: "c-1".to_string(),
                value: "z-1".to_string(),
            },
        ];

        assert_eq!(
            updated_labels,
            &expected_labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect::<Labels>()
        );

        let block = entry.block_manager.write().unwrap().load_block(1).unwrap();
        let mut record = block.read().unwrap().get_record(1).unwrap().clone();
        record.labels.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(record.labels, expected_labels);
    }
    fn make_update(time: u64) -> UpdateLabels {
        UpdateLabels {
            time: time,
            update: Labels::from_iter(vec![
                (format!("a-{}", time), format!("y-{}", time)),
                (format!("b-{}", time), format!("f-{}", time)),
            ]),
            remove: HashSet::from_iter(vec![format!("c-{}", time), "".to_string()]),
        }
    }

    fn make_expected_labels(time: u64) -> Labels {
        Labels::from_iter(vec![
            (format!("a-{}", time), format!("y-{}", time)),
            (format!("b-{}", time), format!("f-{}", time)),
        ])
    }

    async fn write_stub_record(mut entry: &mut Entry, time: u64) {
        write_record_with_labels(
            &mut entry,
            time,
            vec![],
            Labels::from_iter(vec![
                (format!("a-{}", time), format!("x-{}", time)),
                (format!("c-{}", time), format!("z-{}", time)),
            ]),
        )
        .await;
    }
}
