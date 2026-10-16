// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::bucket::Bucket;
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::QuotaType;
use reduct_base::{bad_request, internal_server_error};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

impl Bucket {
    pub(super) async fn keep_quota_for(
        self: &Arc<Self>,
        content_size: u64,
    ) -> Result<(), ReductError> {
        let (quota_size, quota_type) = {
            let settings = self.settings.read().await?;
            (
                settings.quota_size.unwrap_or(0),
                settings.quota_type.clone().unwrap_or(QuotaType::NONE),
            )
        };

        match quota_type {
            QuotaType::NONE => Ok(()),
            QuotaType::FIFO => self.remove_oldest_block(content_size, quota_size).await,
            QuotaType::HARD => {
                let entries = self.entries.read().await?;
                let mut total_size = 0u64;
                for entry in entries.values() {
                    total_size += entry.size().await?;
                }
                if total_size + content_size as u64 > quota_size {
                    Err(bad_request!("Quota of '{}' exceeded", self.name()))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn remove_oldest_block(
        &self,
        content_size: u64,
        quota_size: u64,
    ) -> Result<(), ReductError> {
        let entries = {
            let entries = self.entries.read().await?;
            entries
                .iter()
                .map(|(name, entry)| (name.clone(), Arc::clone(entry)))
                .collect::<HashMap<_, _>>()
        };

        let mut candidates = BinaryHeap::new();
        let mut size = content_size;
        for (name, entry) in &entries {
            size += entry.size().await?;
            if entry.is_eligible_for_fifo_eviction() {
                if let Some((block_id, _)) = entry.oldest_block().await? {
                    candidates.push(Reverse((block_id, name.clone())));
                }
            }
        }

        while size > quota_size {
            let Some(Reverse((_, name))) = candidates.pop() else {
                return Err(internal_server_error!(
                    "Failed to keep quota of '{}'",
                    self.name()
                ));
            };

            let entry = entries
                .get(&name)
                .expect("FIFO eviction candidate must reference a bucket entry");
            debug!(
                "Need more space. Remove an oldest block from entry '{}'",
                name
            );

            match entry.try_remove_oldest_block().await {
                Ok(removed_size) => {
                    size = size.checked_sub(removed_size).ok_or_else(|| {
                        internal_server_error!(
                            "FIFO eviction size underflow in bucket '{}'",
                            self.name()
                        )
                    })?;

                    if let Some((block_id, _)) = entry.oldest_block().await? {
                        candidates.push(Reverse((block_id, name)));
                    }
                }
                Err(e) => {
                    debug!("Failed to remove oldest block from entry '{}': {}", name, e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::cfg::Cfg;
    use crate::storage::bucket::tests::{bucket, path, write, write_meta};
    use crate::storage::bucket::Bucket;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::rstest;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_fifo_quota_keeping(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(20),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(120),
                max_block_records: Some(100),
            },
            path,
        )
        .await;

        let blob: &[u8] = &[0u8; 40];

        write(&bucket, "test-1", 0, blob).await.unwrap();
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 44);

        write(&bucket, "test-2", 1, blob).await.unwrap();
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 91);

        write(&bucket, "test-3", 2, blob).await.unwrap();
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 94);

        assert_eq!(
            crate::storage::bucket::tests::read(&bucket, "test-1", 0)
                .await
                .err(),
            Some(ReductError::not_found(
                "Record 0 not found in entry test/test-1"
            ))
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fifo_quota_ignores_meta_entries_for_eviction(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(20),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(120),
                max_block_records: Some(100),
            },
            path,
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        write_meta(&bucket, "data-1/$meta", 0, blob).await.unwrap();
        write(&bucket, "data-1", 1, blob).await.unwrap();
        write(&bucket, "data-2", 2, blob).await.unwrap();

        assert!(crate::storage::bucket::tests::read(&bucket, "data-1", 1)
            .await
            .is_err());
        assert!(
            crate::storage::bucket::tests::read(&bucket, "data-1/$meta", 0)
                .await
                .is_ok()
        );
        assert!(crate::storage::bucket::tests::read(&bucket, "data-2", 2)
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fifo_quota_refreshes_only_evicted_entry_candidate(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                max_block_records: Some(1),
                ..BucketSettings::default()
            },
            path,
        )
        .await;
        let blob: &[u8] = &[0; 40];

        write(&bucket, "first", 1, blob).await.unwrap();
        write(&bucket, "second", 2, blob).await.unwrap();
        write(&bucket, "first", 3, blob).await.unwrap();

        bucket.remove_oldest_block(0, 60).await.unwrap();

        assert!(crate::storage::bucket::tests::read(&bucket, "first", 1)
            .await
            .is_err());
        assert!(crate::storage::bucket::tests::read(&bucket, "second", 2)
            .await
            .is_err());
        assert!(crate::storage::bucket::tests::read(&bucket, "first", 3)
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_hard_quota_keeping(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::HARD),
                quota_size: Some(100),
                ..BucketSettings::default()
            },
            path,
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        write(&bucket, "test-1", 0, blob).await.unwrap();
        write(&bucket, "test-2", 1, blob).await.unwrap();

        let err = write(&bucket, "test-3", 2, blob).await.err().unwrap();
        assert_eq!(err, ReductError::bad_request("Quota of 'test' exceeded"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_blob_bigger_than_quota(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(5),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(10),
                max_block_records: Some(100),
            },
            path,
        )
        .await;

        write(&bucket, "test-1", 0, b"test").await.unwrap();
        bucket.sync_fs().await.unwrap(); // we need to sync to get the correct size
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 24);

        let result = write(&bucket, "test-2", 1, b"0123456789___").await;
        assert_eq!(
            result.err(),
            Some(ReductError::internal_server_error(
                "Failed to keep quota of 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_fifo_quota_removes_compressed_oldest_block(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                max_block_records: Some(1),
                ..BucketSettings::default()
            },
            path,
        )
        .await;
        let blob: &[u8] = &[0; 40];

        write(&bucket, "entry", 1, blob).await.unwrap();
        write(&bucket, "entry", 2, blob).await.unwrap();
        bucket.sync_fs().await.unwrap();
        let bucket = Arc::new(
            Bucket::builder()
                .path(bucket.path.clone())
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .restore()
                .await
                .unwrap(),
        );
        bucket
            .clone()
            .compress_blocks(Some(vec!["entry".into()]), None, Some(2))
            .await
            .unwrap();

        let compressed_data_path = bucket.path.join("entry/1.blk.zst");
        let compressed_desc_path = bucket.path.join("entry/1.meta.zst");
        assert!(compressed_data_path.exists());
        assert!(compressed_desc_path.exists());

        let size = bucket.clone().info().await.unwrap().info.size;
        bucket
            .set_settings(BucketSettings {
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(size + blob.len() as u64 - 1),
                max_block_records: Some(1),
                ..BucketSettings::default()
            })
            .await
            .unwrap();
        write(&bucket, "entry", 3, blob).await.unwrap();

        assert!(crate::storage::bucket::tests::read(&bucket, "entry", 1)
            .await
            .is_err());
        assert!(!compressed_data_path.exists());
        assert!(!compressed_desc_path.exists());
        assert!(bucket.clone().info().await.unwrap().info.size <= size + blob.len() as u64 - 1);
    }
}
