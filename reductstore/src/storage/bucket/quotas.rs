// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::bucket::Bucket;
use log::debug;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::bucket_api::QuotaType;
use reduct_base::{bad_request, internal_server_error};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

impl Bucket {
    /// Ensure the filesystem that holds the data folder has enough free space to
    /// store an incoming record of `content_size` bytes.
    ///
    /// This complements the configured quota: even when the bucket is within its
    /// quota, the write is rejected early if the underlying filesystem cannot fit
    /// the record. The check runs before any data is written.
    pub(super) async fn check_free_disk_space(&self, content_size: u64) -> Result<(), ReductError> {
        let path = self.path.clone();
        let free_space_fn = self.free_space_fn.clone();

        // `available_space` may perform a blocking syscall, so keep it off the async reactor.
        let available = tokio::task::spawn_blocking(move || free_space_fn(&path))
            .await
            .map_err(|e| internal_server_error!("Failed to query free disk space: {}", e))?
            .map_err(|e| {
                internal_server_error!("Failed to query free disk space for the data folder: {}", e)
            })?;

        if content_size > available {
            return Err(ReductError::new(
                ErrorCode::InsufficientStorage,
                &format!(
                    "Not enough free disk space in the data folder to write a record of {} bytes: only {} bytes available",
                    content_size, available
                ),
            ));
        }

        Ok(())
    }

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
            QuotaType::FIFO => {
                #[cfg(test)]
                if let Some(notify) = self.quota_wait_notify.write().await?.take() {
                    notify.notify_one();
                }

                // Remote block deletion can take substantial time. Serialize cleanup so
                // concurrent writes do not queue stale removals for the same block.
                let _cleanup = self.quota_cleanup.write().await?;

                let settings = self.settings.read().await?;
                match settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
                    QuotaType::NONE => Ok(()),
                    QuotaType::FIFO => self.remove_oldest_block(content_size).await,
                    QuotaType::HARD => {
                        let quota_size = settings.quota_size.unwrap_or(0);
                        self.ensure_hard_quota(content_size, quota_size).await
                    }
                }
            }
            QuotaType::HARD => self.ensure_hard_quota(content_size, quota_size).await,
        }
    }

    async fn ensure_hard_quota(
        &self,
        content_size: u64,
        quota_size: u64,
    ) -> Result<(), ReductError> {
        let entries = self.entries.read().await?;
        let mut total_size = 0u64;
        for entry in entries.values() {
            total_size += entry.size().await?;
        }
        if total_size + content_size > quota_size {
            Err(bad_request!("Quota of '{}' exceeded", self.name()))
        } else {
            Ok(())
        }
    }

    async fn remove_oldest_block(&self, content_size: u64) -> Result<(), ReductError> {
        let entries = {
            let entries = self.entries.read().await?;
            entries
                .iter()
                .map(|(name, entry)| (name.clone(), Arc::clone(entry)))
                .collect::<HashMap<_, _>>()
        };

        let mut candidates = BinaryHeap::new();
        let mut candidates_ready = false;
        let mut size = content_size;

        loop {
            let _admission = Arc::clone(&self.maintenance_admission).read_owned().await?;

            self.ensure_not_deleting().await?;
            let settings = self.settings.read().await?;
            let quota_type = settings.quota_type.clone().unwrap_or(QuotaType::NONE);
            let quota_size = settings.quota_size.unwrap_or(0);

            if !candidates_ready {
                for (name, entry) in &entries {
                    size += entry.size().await?;
                    if entry.is_eligible_for_fifo_eviction() {
                        if let Some((block_id, _)) = entry.oldest_block().await? {
                            candidates.push(Reverse((block_id, name.clone())));
                        }
                    }
                }
                candidates_ready = true;
            }

            match quota_type {
                QuotaType::NONE => return Ok(()),
                QuotaType::HARD => {
                    return if size > quota_size {
                        Err(bad_request!("Quota of '{}' exceeded", self.name()))
                    } else {
                        Ok(())
                    };
                }
                QuotaType::FIFO => {}
            }

            if size <= quota_size {
                return Ok(());
            }

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

            #[cfg(test)]
            if let Some(pause) = self.fifo_pause.write().await?.take() {
                pause.started.notify_one();
                pause.release.notified().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cfg::Cfg;
    use crate::core::file_cache::build_test_file_cache;
    use crate::storage::bucket::tests::{bucket, path, read, write, write_meta};
    use crate::storage::bucket::{Bucket, FifoPause, FreeSpaceFn};
    use reduct_base::conflict;
    use reduct_base::error::{ErrorCode, ReductError};
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::rstest;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    async fn bucket_with_two_blocks(path: PathBuf, quota_size: u64) -> Arc<Bucket> {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(100),
                quota_type: Some(QuotaType::NONE),
                max_block_records: Some(1),
                ..BucketSettings::default()
            },
            path,
        )
        .await;
        write(&bucket, "entry", 1, b"aaaa").await.unwrap();
        write(&bucket, "entry", 2, b"bbbb").await.unwrap();
        bucket.sync_fs().await.unwrap();
        *bucket.settings.write().await.unwrap() = BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(quota_size),
            max_block_records: Some(1),
        };
        bucket
    }

    async fn entry_block_count(bucket: &Arc<Bucket>, entry_name: &str) -> u64 {
        let entry = {
            let entries = bucket.entries.read().await.unwrap();
            entries.get(entry_name).cloned().unwrap()
        };
        entry.info().await.unwrap().block_count
    }

    /// Build a bucket whose free-space provider reports a fixed number of
    /// available bytes, so the disk-space gate can be exercised deterministically.
    async fn bucket_with_free_space(
        settings: BucketSettings,
        path: PathBuf,
        free_space_fn: FreeSpaceFn,
    ) -> Arc<Bucket> {
        let file_cache = build_test_file_cache();
        file_cache.create_dir_all(&path.join("test")).await.unwrap();
        Arc::new(
            Bucket::builder()
                .name("test")
                .data_path(path)
                .settings(settings)
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .free_space_fn(free_space_fn)
                .file_cache(file_cache)
                .build()
                .await
                .unwrap(),
        )
    }

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

        *bucket.settings.write().await.unwrap() = BucketSettings {
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(60),
            max_block_records: Some(1),
            ..BucketSettings::default()
        };
        bucket.remove_oldest_block(0).await.unwrap();

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
    async fn test_write_rejected_when_disk_full(path: PathBuf) {
        // Simulate a filesystem that only has 10 bytes of free space left.
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(|_| Ok(10)),
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        let err = write(&bucket, "test-1", 0, blob).await.err().unwrap();

        assert_eq!(err.status, ErrorCode::InsufficientStorage);
        assert_eq!(
            err.message,
            "Not enough free disk space in the data folder to write a record of 40 bytes: only 10 bytes available"
        );

        // The write must be rejected before any data is stored.
        assert!(
            read(&bucket, "test-1", 0).await.is_err(),
            "no record should have been written when the disk is full"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_allowed_when_disk_has_space(path: PathBuf) {
        // Plenty of free disk space: the write must succeed as before.
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(|_| Ok(1_000_000)),
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        write(&bucket, "test-1", 0, blob).await.unwrap();
        assert!(read(&bucket, "test-1", 0).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_disk_full_error_surfaces_provider_failure(path: PathBuf) {
        // A failure while querying free space is reported as an internal error.
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(|_| Err(std::io::Error::other("boom"))),
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        let err = write(&bucket, "test-1", 0, blob).await.err().unwrap();
        assert_eq!(err.status, ErrorCode::InternalServerError);
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

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn fifo_cleanup_queued_behind_cleanup_lock_observes_deletion(path: PathBuf) {
        let bucket = bucket_with_two_blocks(path, 0).await;
        let wait = Arc::new(Notify::new());
        bucket.set_quota_wait_notify(Some(wait.clone())).await;

        let cleanup_guard = bucket.quota_cleanup.write().await.unwrap();
        let cleanup = tokio::spawn({
            let bucket = Arc::clone(&bucket);
            async move { bucket.keep_quota_for(0).await }
        });
        timeout(Duration::from_secs(2), wait.notified())
            .await
            .expect("cleanup did not reach the quota cleanup lock");

        bucket.mark_deleting().await.unwrap();
        drop(cleanup_guard);

        let err = cleanup.await.unwrap().unwrap_err();
        assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        assert_eq!(entry_block_count(&bucket, "entry").await, 2);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn fifo_cleanup_yields_to_queued_deletion_at_block_boundary(path: PathBuf) {
        let bucket = bucket_with_two_blocks(path, 0).await;
        let pause = Arc::new(FifoPause {
            started: Notify::new(),
            release: Notify::new(),
        });
        bucket.set_fifo_pause(Some(pause.clone())).await;

        let cleanup = tokio::spawn({
            let bucket = Arc::clone(&bucket);
            async move { bucket.keep_quota_for(0).await }
        });
        timeout(Duration::from_secs(2), pause.started.notified())
            .await
            .expect("cleanup did not pause after the first block");
        assert_eq!(entry_block_count(&bucket, "entry").await, 1);

        let deletion_waiter = tokio::spawn({
            let admission = Arc::clone(&bucket.maintenance_admission);
            async move { admission.write_owned().await.unwrap() }
        });
        tokio::task::yield_now().await;

        pause.release.notify_one();
        let admission = timeout(Duration::from_secs(2), deletion_waiter)
            .await
            .expect("deletion did not acquire maintenance admission")
            .unwrap();

        bucket.mark_deleting().await.unwrap();
        assert_eq!(entry_block_count(&bucket, "entry").await, 1);
        drop(admission);

        let err = cleanup.await.unwrap().unwrap_err();
        assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        assert_eq!(entry_block_count(&bucket, "entry").await, 1);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn fifo_cleanup_uses_quota_size_changed_while_waiting(path: PathBuf) {
        let bucket = bucket_with_two_blocks(path, 1_000_000).await;
        let wait = Arc::new(Notify::new());
        bucket.set_quota_wait_notify(Some(wait.clone())).await;

        let cleanup_guard = bucket.quota_cleanup.write().await.unwrap();
        let cleanup = tokio::spawn({
            let bucket = Arc::clone(&bucket);
            async move { bucket.keep_quota_for(0).await }
        });
        timeout(Duration::from_secs(2), wait.notified())
            .await
            .expect("cleanup did not reach the quota cleanup lock");

        *bucket.settings.write().await.unwrap() = BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(0),
            max_block_records: Some(1),
        };
        drop(cleanup_guard);

        cleanup.await.unwrap().unwrap();
        assert_eq!(entry_block_count(&bucket, "entry").await, 0);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn fifo_cleanup_uses_quota_type_changed_while_waiting(path: PathBuf) {
        let bucket = bucket_with_two_blocks(path, 0).await;
        let wait = Arc::new(Notify::new());
        bucket.set_quota_wait_notify(Some(wait.clone())).await;

        let cleanup_guard = bucket.quota_cleanup.write().await.unwrap();
        let cleanup = tokio::spawn({
            let bucket = Arc::clone(&bucket);
            async move { bucket.keep_quota_for(0).await }
        });
        timeout(Duration::from_secs(2), wait.notified())
            .await
            .expect("cleanup did not reach the quota cleanup lock");

        *bucket.settings.write().await.unwrap() = BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::HARD),
            quota_size: Some(1_000_000),
            max_block_records: Some(1),
        };
        drop(cleanup_guard);

        cleanup.await.unwrap().unwrap();
        assert_eq!(entry_block_count(&bucket, "entry").await, 2);
    }
}
