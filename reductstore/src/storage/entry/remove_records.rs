// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::thread_pool::{TaskHandle, THREAD_POOL};
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::Entry;
use crate::storage::query::base::QueryOptions;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::not_found;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use tokio::fs::read_to_string;

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
        let bucket_name = self.bucket_name.clone();
        let entry_name = self.name.clone();
        THREAD_POOL.unique(&format!("{}/{}", self.bucket_name, self.name), move || {
            Self::inner_remove_records(timestamps, block_manager, &bucket_name, &entry_name)?
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
        start: u64,
        end: u64,
        mut options: QueryOptions,
    ) -> TaskHandle<Result<u64, ReductError>> {
        options.continuous = false; // force non-continuous query

        let rx = || {
            let query_id = self.query(start, end, options).wait()?;
            self.get_query_receiver(query_id)
        };

        let rx = match rx() {
            Ok(rx) => rx,
            Err(e) => return Err(e).into(),
        };

        let block_manager = self.block_manager.clone();
        let bucket_name = self.bucket_name.clone();
        let entry_name = self.name.clone();

        let max_block_records = self.settings().max_block_records; // max records per block
        THREAD_POOL.shared(&format!("{}/{}", self.bucket_name, self.name), move || {
            // Loop until the query is done
            let mut continue_query = true;
            let mut handlers = vec![];
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
                let copy_bucket_name = bucket_name.clone();
                let copy_entry_name = entry_name.clone();
                let copy_block_manager = block_manager.clone();
                let handler = THREAD_POOL.unique(
                    &format!("{}/{}/remove", bucket_name, entry_name),
                    move || {
                        Self::inner_remove_records(
                            records_to_remove,
                            copy_block_manager,
                            &copy_bucket_name,
                            &copy_entry_name,
                        )
                    },
                );

                handlers.push(handler);
            }

            // Wait for all handlers to finish
            for handler in handlers {
                handler.wait()??;
            }

            Ok(total_records)
        })
    }

    fn inner_remove_records(
        timestamps: Vec<u64>,
        block_manager: Arc<RwLock<BlockManager>>,
        bucket_name: &String,
        entry_name: &String,
    ) -> Result<Result<BTreeMap<u64, ReductError>, ReductError>, ReductError> {
        let mut error_map = BTreeMap::new();
        let mut records_per_block = BTreeMap::new();

        {
            let bm = block_manager.read()?;
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
            let handler = THREAD_POOL.unique(
                &format!("{}/{}/{}", bucket_name, entry_name, block_id),
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

        Ok(Ok(error_map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::tests::{entry, write_stub_record};
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_remove_records(#[future] mut entry_with_data: Entry) {
        let entry = entry_with_data.await;
        let timestamps = vec![0, 2, 4, 5];
        let error_map = entry.remove_records(timestamps).await.unwrap();

        assert_eq!(error_map.len(), 2, "Only two records are not found");
        assert_eq!(error_map[&0], not_found!("No record with timestamp 0"));
        assert_eq!(error_map[&5], not_found!("No record with timestamp 5"));

        // check existing records
        assert!(entry.begin_read(1).await.is_ok());
        assert!(entry.begin_read(3).await.is_ok());

        // check removed records
        assert_eq!(
            entry.begin_read(2).await.err().unwrap(),
            not_found!("No record with timestamp 2")
        );
        assert_eq!(
            entry.begin_read(4).await.err().unwrap(),
            not_found!("No record with timestamp 4")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_remove_records(#[future] mut entry_with_data: Entry) {
        let mut entry = entry_with_data.await;
        let removed_records = entry
            .query_remove_records(2, 4, QueryOptions::default())
            .await
            .unwrap();

        assert_eq!(removed_records, 2);

        // check removed records
        assert_eq!(
            entry.begin_read(2).await.err().unwrap(),
            not_found!("No record with timestamp 2")
        );
        assert_eq!(
            entry.begin_read(3).await.err().unwrap(),
            not_found!("No record with timestamp 3")
        );
    }

    #[fixture]
    async fn entry_with_data(mut entry: Entry) -> Entry {
        entry.settings.max_block_records = 2;

        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2).await.unwrap();
        write_stub_record(&mut entry, 3).await.unwrap();
        write_stub_record(&mut entry, 4).await.unwrap();
        entry
    }
}
