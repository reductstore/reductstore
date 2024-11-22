// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::bucket::Bucket;
use crate::storage::entry::Entry;
use log::{debug, info, warn};
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::QuotaType;
use reduct_base::{bad_request, internal_server_error};

impl Bucket {
    pub(super) fn keep_quota_for(&self, content_size: usize) -> Result<(), ReductError> {
        let settings = self.settings.read()?;
        let quota_size = settings.quota_size.unwrap_or(0);
        match settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
            QuotaType::NONE => Ok(()),
            QuotaType::FIFO => self.remove_oldest_block(content_size, quota_size),
            QuotaType::HARD => {
                if self.info()?.info.size + content_size as u64 > quota_size {
                    Err(bad_request!("Quota of '{}' exceeded", self.name()))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn remove_oldest_block(&self, content_size: usize, quota_size: u64) -> Result<(), ReductError> {
        let mut size = self.info()?.info.size + content_size as u64;
        while size > quota_size {
            debug!(
                "Need more space. Remove an oldest block from bucket '{}'",
                self.name()
            );

            let mut candidates: Vec<(u64, &Entry)> = vec![];
            let entries = self.entries.read()?;
            for (_, entry) in entries.iter() {
                let info = entry.info()?;
                candidates.push((info.oldest_record, entry));
            }
            candidates.sort_by_key(|entry| entry.0);

            let candidates = candidates
                .iter()
                .map(|(_, entry)| entry.name().to_string())
                .collect::<Vec<String>>();

            let mut success = false;
            for name in candidates {
                debug!("Remove an oldest block from entry '{}'", name);
                match entries.get(&name).unwrap().try_remove_oldest_block().wait() {
                    Ok(_) => {
                        success = true;
                        break;
                    }
                    Err(e) => {
                        warn!("Failed to remove oldest block from entry '{}': {}", name, e);
                    }
                }
            }

            if !success {
                return Err(internal_server_error!(
                    "Failed to keep quota of '{}'",
                    self.name()
                ));
            }

            size = self.info()?.info.size + content_size as u64;
        }

        // Remove empty entries
        let mut entries = self.entries.write()?;
        let mut names_to_remove = vec![];
        for (name, entry) in entries.iter() {
            if entry.info()?.record_count != 0 {
                continue;
            }
            names_to_remove.push(name.clone());
        }

        for name in names_to_remove {
            entries.remove(&name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::bucket::tests::{bucket, path, write};
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::rstest;
    use std::path::PathBuf;

    #[rstest]
    #[tokio::test]
    async fn test_fifo_quota_keeping(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                max_block_size: Some(20),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(100),
                max_block_records: Some(100),
            },
            path,
        );

        let blob: &[u8] = &[0u8; 40];

        write(&mut bucket, "test-1", 0, blob).await.unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 44);

        write(&mut bucket, "test-2", 1, blob).await.unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 91);

        write(&mut bucket, "test-3", 2, blob).await.unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 94);

        assert_eq!(
            crate::storage::bucket::tests::read(&mut bucket, "test-1", 0)
                .await
                .err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_hard_quota_keeping(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::HARD),
                quota_size: Some(100),
                ..BucketSettings::default()
            },
            path,
        );

        let blob: &[u8] = &[0u8; 40];
        write(&mut bucket, "test-1", 0, blob).await.unwrap();
        write(&mut bucket, "test-2", 1, blob).await.unwrap();

        let err = write(&mut bucket, "test-3", 2, blob).await.err().unwrap();
        assert_eq!(err, ReductError::bad_request("Quota of 'test' exceeded"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_blob_bigger_than_quota(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                max_block_size: Some(5),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(10),
                max_block_records: Some(100),
            },
            path,
        );

        write(&mut bucket, "test-1", 0, b"test").await.unwrap();
        bucket.sync_fs().await.unwrap(); // we need to sync to get the correct size
        assert_eq!(bucket.info().unwrap().info.size, 22);

        let result = write(&mut bucket, "test-2", 1, b"0123456789___").await;
        assert_eq!(
            result.err(),
            Some(ReductError::internal_server_error(
                "Failed to keep quota of 'test'"
            ))
        );
    }
}
