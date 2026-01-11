// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::bucket::Bucket;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryEntry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

impl Bucket {
    /// Remove records from the bucket
    ///
    /// # Arguments
    ///
    /// * `record_ids` - A map where the key is the entry name and the value is a vector of record IDs to remove.
    pub async fn remove_records(
        self: Arc<Self>,
        record_ids: HashMap<String, Vec<u64>>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut results = BTreeMap::new();

        for (entry_name, ids) in record_ids {
            match self.get_entry(&entry_name).await {
                Ok(entry) => {
                    let entry = entry.upgrade()?;
                    let entry_results = entry.remove_records(ids).await?;
                    results.extend(entry_results);
                }
                Err(e) => {
                    for id in ids {
                        results.insert(id, e.clone());
                    }
                }
            }
        }

        Ok(results)
    }

    /// Query and remove multiple records over a range of timestamps.
    ///
    /// # Arguments
    ///
    /// * `options` - The query options.
    ///
    /// # Returns
    /// The number of records removed.
    pub async fn query_remove_records(
        self: Arc<Self>,
        options: QueryEntry,
    ) -> Result<u64, ReductError> {
        let entries = self.entries.read().await?.clone();
        let mut total_removed = 0;

        for (entry_name, entry) in entries {
            if let Some(requested) = &options.entries {
                if !requested.contains(&entry_name) {
                    continue;
                }
            }

            let removed_records = entry.query_remove_records(options.clone()).await?;
            total_removed += removed_records;
        }

        Ok(total_removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bucket::tests::{bucket, write};
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use reduct_base::not_found;
    use rstest::rstest;
    use std::collections::HashMap;

    #[rstest]
    #[tokio::test]
    async fn removes_records_from_multiple_entries(bucket: Arc<Bucket>) {
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-b", 2, b"b1").await.unwrap();
        write(&bucket, "entry-b", 3, b"b2").await.unwrap();

        let errors = bucket
            .remove_records(HashMap::from([
                ("entry-a".to_string(), vec![1]),
                ("entry-b".to_string(), vec![2, 4]),
                ("missing".to_string(), vec![5]),
            ]))
            .await
            .unwrap();

        assert_eq!(errors.len(), 2);
        assert_eq!(errors[&4], not_found!("No record with timestamp 4"));
        assert_eq!(
            errors[&5],
            not_found!("Entry 'missing' not found in bucket 'test'")
        );

        assert_eq!(
            bucket.begin_read("entry-a", 1).await.err().unwrap(),
            not_found!("No record with timestamp 1")
        );
        assert_eq!(
            bucket.begin_read("entry-b", 2).await.err().unwrap(),
            not_found!("No record with timestamp 2")
        );
        assert!(bucket.begin_read("entry-b", 3).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn query_remove_records_filters_entries(bucket: Arc<Bucket>) {
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-a", 4, b"a2").await.unwrap();
        write(&bucket, "entry-b", 2, b"b1").await.unwrap();
        write(&bucket, "entry-c", 2, b"c1").await.unwrap();

        let request = QueryEntry {
            query_type: QueryType::Remove,
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            start: Some(1),
            stop: Some(3),
            ..Default::default()
        };

        let removed = bucket.clone().query_remove_records(request).await.unwrap();
        assert_eq!(removed, 2);

        assert_eq!(
            bucket.begin_read("entry-a", 1).await.err().unwrap(),
            not_found!("No record with timestamp 1")
        );
        assert_eq!(
            bucket.begin_read("entry-b", 2).await.err().unwrap(),
            not_found!("No record with timestamp 2")
        );
        assert!(bucket.begin_read("entry-a", 4).await.is_ok());
        assert!(bucket.begin_read("entry-c", 2).await.is_ok());
    }
}
