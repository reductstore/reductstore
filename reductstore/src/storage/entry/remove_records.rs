// Copyright 2024-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::Entry;
use log::warn;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::ReadRecord;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::not_found;
use std::collections::BTreeMap;
use std::sync::Arc;

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
    pub async fn remove_records(
        self: Arc<Self>,
        timestamps: Vec<u64>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let block_manager = self.block_manager.clone();
        Self::inner_remove_records(timestamps, block_manager).await
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
    pub async fn query_remove_records(&self, mut options: QueryEntry) -> Result<u64, ReductError> {
        options.continuous = None; // force non-continuous query

        let rx = async || {
            // io defaults isn't used in remove queries
            let query_id = self.query(options).await?;
            self.get_query_receiver(query_id).await
        };

        let rx = match rx().await {
            Ok((rx, _)) => rx,
            Err(e) => return Err(e).into(),
        };

        let block_manager = self.block_manager.clone();
        let max_block_records = self.settings().await?.max_block_records; // max records per block

        // Loop until the query is done
        let mut continue_query = true;
        let mut total_records = 0;

        while continue_query {
            let mut records_to_remove = vec![];
            records_to_remove.reserve(max_block_records as usize);

            // Receive a batch of records to remove
            while records_to_remove.len() < max_block_records as usize && continue_query {
                let result = &mut rx.upgrade()?.write().await?.recv().await;
                match result {
                    Some(Ok(rec)) => {
                        records_to_remove.push(rec.meta().timestamp());
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
                    Some(Err(e)) => return Err(e.clone()),
                }
            }

            // Send the records to remove
            total_records += records_to_remove.len() as u64;
            let copy_block_manager = block_manager.clone();

            match Self::inner_remove_records(records_to_remove, copy_block_manager).await {
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
    }

    async fn inner_remove_records(
        timestamps: Vec<u64>,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut error_map = BTreeMap::new();
        let mut records_per_block = BTreeMap::new();

        {
            for time in timestamps {
                // Find the block that contains the record
                // TODO: Try to avoid the lookup for each record
                match block_manager.write().await?.find_block(time).await {
                    Ok(block_ref) => {
                        // Check if the record exists
                        let block = block_ref.read().await?;
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
            let handler = tokio::spawn(async move {
                // TODO: we don't parallelize the removal of records in different blocks
                let mut bm = local_block_manager.write().await?;
                bm.remove_records(block_id, timestamps).await?;
                Ok::<(), ReductError>(())
            });
            handlers.push(handler);
        }

        for handler in handlers {
            handler.await.unwrap()?;
        }

        Ok(error_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::{
        reset_rwlock_config, set_rwlock_failure_action, set_rwlock_timeout, RwLockFailureAction,
    };
    use crate::storage::entry::tests::{entry, write_stub_record};
    use crate::storage::entry::EntrySettings;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_remove_records(#[future] entry_with_data: Arc<Entry>) {
        let entry_with_data = entry_with_data.await;

        let timestamps = vec![0, 2, 4, 5];
        let error_map = entry_with_data
            .clone()
            .remove_records(timestamps)
            .await
            .unwrap();

        assert_eq!(error_map.len(), 2, "Only two records are not found");
        assert_eq!(error_map[&0], not_found!("No record with timestamp 0"));
        assert_eq!(error_map[&5], not_found!("No record with timestamp 5"));

        // check existing records
        assert!(entry_with_data.begin_read(1).await.is_ok());
        assert!(entry_with_data.begin_read(3).await.is_ok());

        // check removed records
        assert_eq!(
            entry_with_data.begin_read(2).await.err().unwrap(),
            not_found!("Record 2 not found in block bucket/entry/1")
        );
        assert_eq!(
            entry_with_data.begin_read(4).await.err().unwrap(),
            not_found!("Record 4 not found in block bucket/entry/3")
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_query_remove_records(#[future] entry_with_data: Arc<Entry>) {
        let entry_with_data = entry_with_data.await;

        let params = QueryEntry {
            start: Some(2),
            stop: Some(4),
            ..Default::default()
        };

        let removed_records = entry_with_data.query_remove_records(params).await.unwrap();

        assert_eq!(removed_records, 2);

        // check removed records
        assert_eq!(
            entry_with_data.begin_read(2).await.err().unwrap(),
            not_found!("Record 2 not found in block bucket/entry/1")
        );
        assert_eq!(
            entry_with_data.begin_read(3).await.err().unwrap(),
            not_found!("Record 3 not found in block bucket/entry/3")
        );
    }

    // TODO: replace with multiple add/remove on RwLock
    #[fixture]
    async fn entry_with_data(entry: Arc<Entry>) -> Arc<Entry> {
        struct ResetGuard;
        impl Drop for ResetGuard {
            fn drop(&mut self) {
                reset_rwlock_config();
            }
        }
        let _reset = ResetGuard;
        set_rwlock_failure_action(RwLockFailureAction::Error);
        set_rwlock_timeout(Duration::from_secs(10));

        entry
            .set_settings(EntrySettings {
                max_block_records: 2,
                ..entry.settings().await.unwrap()
            })
            .await
            .unwrap();

        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2).await;
        write_stub_record(&entry, 3).await;
        write_stub_record(&entry, 4).await;
        entry
    }
}
