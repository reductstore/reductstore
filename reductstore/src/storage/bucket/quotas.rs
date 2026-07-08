// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::bucket::Bucket;
use crate::storage::entry::Entry;
use log::debug;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::bucket_api::QuotaType;
use reduct_base::{bad_request, internal_server_error};
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
        let settings = self.settings.read().await?;
        let quota_size = settings.quota_size.unwrap_or(0);
        match settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
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
        let get_bucket_size = async || {
            let entries = self.entries.read().await?;

            let mut total_size = 0u64;
            for entry in entries.values() {
                total_size += entry.size().await?;
            }
            Ok::<u64, ReductError>(total_size)
        };

        let mut size = get_bucket_size().await? + content_size as u64;
        while size > quota_size {
            let mut success = false;

            {
                debug!(
                    "Need more space. Remove an oldest block from bucket '{}'",
                    self.name()
                );

                let mut candidates: Vec<(u64, &Entry)> = vec![];
                let entries = self.entries.read().await?;
                for (_, entry) in entries.iter() {
                    if !entry.is_eligible_for_fifo_eviction() {
                        continue;
                    }
                    let info = entry.info().await?;
                    candidates.push((info.oldest_record, entry));
                }
                candidates.sort_by_key(|entry| entry.0);

                let candidates = candidates
                    .iter()
                    .map(|(_, entry)| entry.name().to_string())
                    .collect::<Vec<String>>();

                for name in candidates {
                    debug!("Remove an oldest block from entry '{}'", name);
                    match entries.get(&name).unwrap().try_remove_oldest_block().await {
                        Ok(_) => {
                            success = true;
                            break;
                        }
                        Err(e) => {
                            debug!("Failed to remove oldest block from entry '{}': {}", name, e);
                        }
                    }
                }
            }

            if !success {
                return Err(internal_server_error!(
                    "Failed to keep quota of '{}'",
                    self.name()
                ));
            }

            size = get_bucket_size().await? + content_size;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::bucket::tests::{bucket, path, read, write, write_meta};
    use crate::storage::bucket::{Bucket, FreeSpaceFn};
    use reduct_base::error::{ErrorCode, ReductError};
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::rstest;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Build a bucket whose free-space provider reports a fixed number of
    /// available bytes, so the disk-space gate can be exercised deterministically.
    async fn bucket_with_free_space(
        settings: BucketSettings,
        path: PathBuf,
        free_space_fn: FreeSpaceFn,
    ) -> Arc<Bucket> {
        FILE_CACHE.create_dir_all(&path.join("test")).await.unwrap();
        Arc::new(
            Bucket::builder()
                .name("test")
                .data_path(path)
                .settings(settings)
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .free_space_fn(free_space_fn)
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
}
