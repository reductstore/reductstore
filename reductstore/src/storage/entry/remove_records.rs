// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::entry::Entry;
use crate::storage::query::base::QueryOptions;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::not_found;
use std::collections::BTreeMap;

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
        &self,
        timestamps: Vec<u64>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut error_map = BTreeMap::new();
        let mut records_per_block = BTreeMap::new();

        {
            let bm = self.block_manager.read().await;
            for time in timestamps {
                // Find the block that contains the record
                // TODO: Try to avoid the lookup for each record
                match bm.find_block(time).await {
                    Ok(block_ref) => {
                        // Check if the record exists
                        let block = block_ref.read().await;
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
        for (block_id, timestamps) in records_per_block {
            let mut bm = self.block_manager.write().await;
            bm.remove_records(block_id, timestamps).await?;
        }

        Ok(error_map)
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
    pub async fn query_remove_records(
        &mut self,
        start: u64,
        end: u64,
        mut options: QueryOptions,
    ) -> Result<u64, ReductError> {
        options.continuous = false; // force non-continuous query
        let query_id = self.query(start, end, options)?;

        let mut total_records = 0;
        let max_block_records = self.settings().max_block_records; // max records per block

        // Loop until the query is done
        let mut continue_query = true;
        while continue_query {
            let rx = self.get_query_receiver(query_id).await?;
            let mut records_to_remove = vec![];
            records_to_remove.reserve(max_block_records as usize);

            // Receive a batch of records to remove
            while records_to_remove.len() < max_block_records as usize && continue_query {
                let result = rx.recv().await;
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

            // Remove the records
            total_records += records_to_remove.len() as u64;
            self.remove_records(records_to_remove).await?;
        }

        Ok(total_records)
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
