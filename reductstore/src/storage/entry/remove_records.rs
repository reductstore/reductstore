// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::thread_pool::{shared, unique, unique_child, TaskHandle};
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::Entry;
use log::warn;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::not_found;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

impl Entry {
    /// Remove multiple records.
    ///
    /// The method removes multiple records. The records are identified by their timestamps
    /// and batched by the block they belong to
    ///
    /// # Arguments
    ///
    /// * `timestamps` - A vector of timestamps of the records to remove.
    ///
    /// # Returns
    ///
    /// A map of timestamps to the result of the remove operation. The result is either a vector of labels
    /// or an error if the record was not found.
    pub fn remove_records(
        &self,
        timestamps: Vec<u64>,
    ) -> TaskHandle<Result<BTreeMap<u64, ReductError>, ReductError>> {
        let block_manager = self.block_manager.clone();
        let task_group = self.task_group();
        unique(&task_group.clone(), "remove records", move || {
            Self::inner_remove_records(timestamps, block_manager, task_group)
        })
    }

    /// Query and remove multiple records over a range of timestamps.
    ///
    /// # Arguments
    ///
    /// * `start` - The start timestamp of the query.
    /// * `end` - The end timestamp of the query.
    /// * `options` - The query options.
    ///
    /// # Returns
    ///
    /// The number of records removed.
    ///
    /// # Errors
    ///
    /// * If the query fails.
    pub fn query_remove_records(
        &self,
        mut options: QueryEntry,
    ) -> TaskHandle<Result<u64, ReductError>> {
        options.continuous = None; // force non-continuous query

        let rx = || {
            let query_id = self.query(options).wait()?;
            self.get_query_receiver(query_id)
        };

        let rx = match rx() {
            Ok(rx) => rx,
            Err(e) => return Err(e).into(),
        };

        let block_manager = self.block_manager.clone();
        let max_block_records = self.settings().max_block_records; // max records per block
        let task_group = self.task_group();

        shared(&task_group.clone(), "query remove records", move || {
            // Loop until the query is done
            let mut continue_query = true;
            let mut total_records = 0;

            while continue_query {
                let mut records_to_remove = vec![];
                records_to_remove.reserve(max_block_records as usize);

                // Receive a batch of records to remove
                while records_to_remove.len() < max_block_records as usize && continue_query {
                    let result = rx.upgrade()?.blocking_write().blocking_recv();
                    match result {
                        Some(Ok(rec)) => {
                            records_to_remove.push(rec.timestamp());
                        }
                        Some(Err(ReductError {
                            status: ErrorCode::NoContent,
                            ..
                        })) => {
                            continue_query = false;
                        }
                        None => {
                            continue_query = false;
                        }
                        Some(Err(e)) => return Err(e),
                    }
                }

                // Send the records to remove
                total_records += records_to_remove.len() as u64;
                let copy_block_manager = block_manager.clone();

                match Self::inner_remove_records(
                    records_to_remove,
                    copy_block_manager,
                    task_group.clone(),
                ) {
                    Ok(error_map) => {
                        for (timestamp, error) in error_map {
                            // TODO: send the error to the client
                            warn!(
                                "Failed to remove record with timestamp {}: {}",
                                timestamp, error
                            );

                            total_records -= 1;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }

            Ok(total_records)
        })
    }

    fn inner_remove_records(
        timestamps: Vec<u64>,
        block_manager: Arc<RwLock<BlockManager>>,
        task_group: String,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut error_map = BTreeMap::new();
        let mut records_per_block = BTreeMap::new();

        {
            let mut bm = block_manager.write()?;
            for time in timestamps {
                // Find the block that contains the record
                // TODO: Try to avoid the lookup for each record
                match bm.find_block(time) {
                    Ok(block_ref) => {
                        // Check if the record exists
                        let block = block_ref.read()?;
                        if let Some(_) = block.get_record(time) {
                            records_per_block
                                .entry(block.block_id())
                                .or_insert_with(Vec::new)
                                .push(time);
                        } else {
                            error_map.insert(time, not_found!("No record with timestamp {}", time));
                        }
                    }
                    Err(e) => {
                        error_map.insert(time, e);
                    }
                }
            }
        }

        // Remove the records
        let mut handlers = vec![];
        for (block_id, timestamps) in records_per_block {
            let local_block_manager = block_manager.clone();
            let handler = unique_child(
                &format!("{}/{}", task_group, block_id),
                "remove records from block",
                move || {
                    // TODO: we don't parallelize the removal of records in different blocks
                    let mut bm = local_block_manager.write().unwrap();
                    bm.remove_records(block_id, timestamps)
                },
            );
            handlers.push(handler);
        }

        for handler in handlers {
            handler.wait()?;
        }

        Ok(error_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::tests::{entry, write_stub_record};
    use crate::storage::entry::EntrySettings;
    use rstest::{fixture, rstest};

    #[rstest]
    fn test_remove_records(entry_with_data: Entry) {
        let timestamps = vec![0, 2, 4, 5];
        let error_map = entry_with_data.remove_records(timestamps).wait().unwrap();

        assert_eq!(error_map.len(), 2, "Only two records are not found");
        assert_eq!(error_map[&0], not_found!("No record with timestamp 0"));
        assert_eq!(error_map[&5], not_found!("No record with timestamp 5"));

        // check existing records
        assert!(entry_with_data.begin_read(1).wait().is_ok());
        assert!(entry_with_data.begin_read(3).wait().is_ok());

        // check removed records
        assert_eq!(
            entry_with_data.begin_read(2).wait().err().unwrap(),
            not_found!("No record with timestamp 2")
        );
        assert_eq!(
            entry_with_data.begin_read(4).wait().err().unwrap(),
            not_found!("No record with timestamp 4")
        );
    }

    #[rstest]
    fn test_query_remove_records(entry_with_data: Entry) {
        let params = QueryEntry {
            start: Some(2),
            stop: Some(4),
            ..Default::default()
        };

        let removed_records = entry_with_data.query_remove_records(params).wait().unwrap();

        assert_eq!(removed_records, 2);

        // check removed records
        assert_eq!(
            entry_with_data.begin_read(2).wait().err().unwrap(),
            not_found!("No record with timestamp 2")
        );
        assert_eq!(
            entry_with_data.begin_read(3).wait().err().unwrap(),
            not_found!("No record with timestamp 3")
        );
    }

    #[fixture]
    fn entry_with_data(mut entry: Entry) -> Entry {
        entry.set_settings(EntrySettings {
            max_block_records: 2,
            ..entry.settings()
        });

        write_stub_record(&mut entry, 1);
        write_stub_record(&mut entry, 2);
        write_stub_record(&mut entry, 3);
        write_stub_record(&mut entry, 4);
        entry
    }
}
