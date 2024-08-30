// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::entry::Entry;
use reduct_base::error::ReductError;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::tests::{entry, write_stub_record};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_remove_records(mut entry: Entry) {
        entry.settings.max_block_records = 2;
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2).await.unwrap();
        write_stub_record(&mut entry, 3).await.unwrap();
        write_stub_record(&mut entry, 4).await.unwrap();

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
}
