use crate::storage::entry::Entry;
use crate::storage::proto::record::Label;
use crate::storage::proto::{ts_to_us, Record};
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::{not_found, Labels};
use std::collections::{BTreeMap, HashSet};

pub(crate) struct UpdateLabels {
    pub time: u64,
    pub update: Labels,
    pub remove: HashSet<String>,
}

impl Entry {
    pub async fn update_labels(
        &self,
        updates: Vec<UpdateLabels>,
    ) -> Result<BTreeMap<u64, Result<Vec<Label>, ReductError>>, ReductError> {
        let mut result = BTreeMap::new();
        let mut records_per_block = BTreeMap::new();

        {
            let bm = self.block_manager.read().await;
            for UpdateLabels {
                time,
                mut update,
                remove,
            } in updates
            {
                // Find the block that contains the record
                // TODO: Try to avoid the lookup for each record
                match bm.find_block(time).await {
                    Ok(block_ref) => {
                        let block = block_ref.read().await;
                        if let Some(record) = block.get_record(time) {
                            records_per_block
                                .entry(block.block_id())
                                .or_insert_with(Vec::new)
                                .push(Self::update_single_label(record.clone(), update, remove));
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
        for (block_id, records) in records_per_block {
            let mut bm = self.block_manager.write().await;
            let mut block_ref = bm.load(block_id).await?;

            let record_times = {
                let mut block = block_ref.write().await;
                let mut record_times = Vec::new();
                for record in records.into_iter() {
                    let time = ts_to_us(record.timestamp.as_ref().unwrap());
                    record_times.push(time);
                    block.insert_or_update_record(record.clone());
                    result.insert(time, Ok(record.labels));
                }
                record_times
            };

            bm.update_records(block_ref.clone(), record_times).await?;
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
    use mockall::predicate::str::contains;
    use rstest::rstest;
    use std::fmt::format;

    #[rstest]
    #[tokio::test]
    async fn test_update_labels(mut entry: Entry) {
        entry.settings.max_block_records = 2;
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2).await.unwrap();
        write_stub_record(&mut entry, 3).await.unwrap();

        // update, remove and add labels
        let mut result = entry
            .update_labels(vec![
                make_update(0),
                make_update(1),
                make_update(2),
                make_update(3),
                make_update(5),
            ])
            .await
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
        assert_eq!(updated_labels.len(), 2);
        assert!(updated_labels.contains(expected_labels_1.get(0).unwrap()));
        assert!(updated_labels.contains(expected_labels_1.get(1).unwrap()));

        let updated_labels = result.get(&2).unwrap().as_ref().unwrap();
        let expected_labels_2 = make_expected_labels(2);
        assert_eq!(updated_labels.len(), 2);
        assert!(updated_labels.contains(expected_labels_2.get(0).unwrap()));
        assert!(updated_labels.contains(expected_labels_2.get(1).unwrap()));

        let updated_labels = result.get(&3).unwrap().as_ref().unwrap();
        let expected_labels_3 = make_expected_labels(3);
        assert_eq!(updated_labels.len(), 2);
        assert!(updated_labels.contains(expected_labels_3.get(0).unwrap()));
        assert!(updated_labels.contains(expected_labels_3.get(1).unwrap()));

        // check if the records were updated
        let block_ref = entry.block_manager.write().await.load(1).await.unwrap();

        let block = block_ref.read().await;
        assert_eq!(block.record_count(), 2);

        let record = block.get_record(1).unwrap().clone();
        assert_eq!(record.labels.len(), 2);
        assert!(record.labels.contains(expected_labels_1.get(0).unwrap()));
        assert!(record.labels.contains(expected_labels_1.get(1).unwrap()));

        let record = block.get_record(2).unwrap().clone();
        assert_eq!(record.labels.len(), 2);
        assert!(record.labels.contains(expected_labels_2.get(0).unwrap()));
        assert!(record.labels.contains(expected_labels_2.get(1).unwrap()));

        let block_ref = entry.block_manager.write().await.load(3).await.unwrap();

        let block = block_ref.read().await;
        assert_eq!(block.record_count(), 1);

        let record = block.get_record(3).unwrap().clone();
        assert_eq!(record.labels.len(), 2);
        assert!(record.labels.contains(expected_labels_3.get(0).unwrap()));
        assert!(record.labels.contains(expected_labels_3.get(1).unwrap()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_nothing(mut entry: Entry) {
        write_stub_record(&mut entry, 1).await.unwrap();
        let result = entry
            .update_labels(vec![UpdateLabels {
                time: 1,
                update: Labels::new(),
                remove: HashSet::new(),
            }])
            .await
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

        assert_eq!(updated_labels.len(), 2);
        assert!(updated_labels.contains(&expected_labels[0]));
        assert!(updated_labels.contains(&expected_labels[1]));

        let block = entry.block_manager.write().await.load(1).await.unwrap();
        let record = block.read().await.get_record(1).unwrap().clone();
        assert_eq!(record.labels.len(), 2);
        assert!(updated_labels.contains(&expected_labels[0]));
        assert!(updated_labels.contains(&expected_labels[1]));
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

    fn make_expected_labels(time: u64) -> Vec<Label> {
        vec![
            Label {
                name: format!("a-{}", time),
                value: format!("y-{}", time),
            },
            Label {
                name: format!("b-{}", time),
                value: format!("f-{}", time),
            },
        ]
    }

    async fn write_stub_record(mut entry: &mut Entry, time: u64) -> Result<(), ReductError> {
        write_record_with_labels(
            &mut entry,
            time,
            vec![],
            Labels::from_iter(vec![
                (format!("a-{}", time), format!("x-{}", time)),
                (format!("c-{}", time), format!("z-{}", time)),
            ]),
        )
        .await
    }
}
