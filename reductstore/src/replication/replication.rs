// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::{RemoteBucket, RemoteBucketImpl};
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use crate::storage::storage::Storage;

use log::{debug, error, info};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::Labels;

use reduct_base::msg::replication_api::ReplicationSettings;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::sleep;

pub struct Replication {
    settings: ReplicationSettings,
    filter: TransactionFilter,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<RwLock<Storage>>,
}

struct ReplicationComponents {
    remote_bucket: Box<dyn RemoteBucket + Send + Sync>,
    filter: TransactionFilter,
    storage: Arc<RwLock<Storage>>,
}

const TRANSACTION_LOG_SIZE: usize = 1000_000;

impl Replication {
    pub fn new(storage: Arc<RwLock<Storage>>, settings: ReplicationSettings) -> Self {
        let ReplicationSettings {
            name: _,
            src_bucket,
            dst_bucket: remote_bucket,
            dst_host: remote_host,
            dst_token: remote_token,
            entries,
            include,
            exclude,
        } = settings.clone();

        let remote_bucket = Box::new(RemoteBucketImpl::new(
            remote_host.as_str(),
            remote_bucket.as_str(),
            remote_token.as_str(),
        ));

        let filter = TransactionFilter::new(src_bucket, entries, include, exclude);

        Self::build(
            ReplicationComponents {
                remote_bucket,
                filter,
                storage,
            },
            settings,
        )
    }

    fn build(replication_components: ReplicationComponents, settings: ReplicationSettings) -> Self {
        let ReplicationComponents {
            mut remote_bucket,
            filter,
            storage,
        } = replication_components;
        let logs = Arc::new(RwLock::new(HashMap::<String, RwLock<TransactionLog>>::new()));
        let map_log = logs.clone();
        let config = settings.clone();

        let local_storage = Arc::clone(&storage);
        tokio::spawn(async move {
            loop {
                for (entry_name, log) in map_log.read().await.iter() {
                    let tr = log.write().await.front().await;
                    match tr {
                        Ok(None) => {
                            // empty log, nothing to do
                            sleep(std::time::Duration::from_micros(100)).await;
                            continue;
                        }
                        Ok(Some(transaction)) => {
                            debug!("Replicating transaction {}/{:?}", entry_name, transaction);

                            let get_record = async {
                                local_storage
                                    .read()
                                    .await
                                    .get_bucket(&config.src_bucket)?
                                    .begin_read(&entry_name, *transaction.timestamp())
                                    .await
                            };

                            let read_record = match get_record.await {
                                Ok(record) => record,
                                Err(err) => {
                                    if err.status == ErrorCode::NotFound {
                                        log.write().await.pop_front().await.unwrap();
                                    }
                                    error!("Failed to read record: {:?}", err);
                                    break;
                                }
                            };

                            match remote_bucket.write_record(entry_name, read_record).await {
                                Ok(_) => {
                                    log.write().await.pop_front().await.unwrap();
                                }
                                Err(err) => {
                                    debug!("Failed to replicate transaction: {:?}", err);
                                    break;
                                }
                            }
                        }

                        Err(err) => {
                            error!("Failed to read transaction: {:?}", err);
                            info!("Transaction log is corrupted, dropping the whole log");
                            let mut log = log.write().await;

                            let path = Self::build_path_to_transaction_log(
                                local_storage.read().await.data_path(),
                                &config.src_bucket,
                                &entry_name,
                                &config.name,
                            );
                            if let Err(err) = fs::remove_file(&path).await {
                                error!("Failed to remove transaction log: {:?}", err);
                            }

                            info!("Creating a new transaction log");
                            *log = TransactionLog::try_load_or_create(path, TRANSACTION_LOG_SIZE)
                                .await
                                .unwrap();
                        }
                    }
                }
            }
        });

        Self {
            settings,
            storage,
            filter,
            log_map: logs,
        }
    }

    pub async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError> {
        if !self.filter.filter(&notification) {
            return Ok(());
        }

        // NOTE: very important not to lock the log_map for too long
        // because it is used by the replication thread
        if !self.log_map.read().await.contains_key(&notification.entry) {
            let mut map = self.log_map.write().await;
            map.insert(
                notification.entry.clone(),
                RwLock::new(
                    TransactionLog::try_load_or_create(
                        Self::build_path_to_transaction_log(
                            self.storage.read().await.data_path(),
                            &self.settings.src_bucket,
                            &notification.entry,
                            &self.settings.name,
                        ),
                        TRANSACTION_LOG_SIZE,
                    )
                    .await?,
                ),
            );
        };

        let log_map = self.log_map.read().await;
        let log = log_map.get(&notification.entry).unwrap();

        if let Some(_) = log
            .write()
            .await
            .push_back(notification.event.clone())
            .await?
        {
            error!("Transaction log is full, dropping the oldest transaction without replication");
        }

        Ok(())
    }

    pub fn name(&self) -> &String {
        &self.settings.name
    }

    pub fn settings(&self) -> &ReplicationSettings {
        &self.settings
    }

    fn build_path_to_transaction_log(
        storage_path: &PathBuf,
        bucket: &str,
        entry: &str,
        name: &str,
    ) -> PathBuf {
        storage_path.join(format!("{}/{}/{}.log", bucket, entry, name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::Transaction;
    use crate::storage::bucket::RecordReader;
    use async_trait::async_trait;
    use bytes::Bytes;
    use mockall::mock;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::*;

    mock! {
        RmBucket {}

        #[async_trait]
        impl RemoteBucket for RmBucket {
            async fn write_record(
                &mut self,
                entry_name: &str,
                record: RecordReader,
            ) -> Result<(), ReductError>;
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        let replication = build_replication(remote_bucket).await;

        replication.notify(notification).await.unwrap();
        assert!(transaction_log_is_empty(replication).await);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_comm_err(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
    ) {
        remote_bucket
            .expect_write_record()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "")));
        let replication = build_replication(remote_bucket).await;

        replication.notify(notification).await.unwrap();
        assert!(
            !transaction_log_is_empty(replication).await,
            "We keep the transaction in the log to sync later"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_not_found(
        mut remote_bucket: MockRmBucket,
        mut notification: TransactionNotification,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        let replication = build_replication(remote_bucket).await;

        notification.event = Transaction::WriteRecord(100);
        replication.notify(notification).await.unwrap();
        assert!(
            transaction_log_is_empty(replication).await,
            "We don't keep the transaction for a non existing record"
        );
    }

    #[fixture]
    fn remote_bucket() -> MockRmBucket {
        MockRmBucket::new()
    }

    #[fixture]
    fn notification() -> TransactionNotification {
        TransactionNotification {
            bucket: "src".to_string(),
            entry: "test".to_string(),
            labels: Labels::new(),
            event: Transaction::WriteRecord(10),
        }
    }

    async fn build_replication(remote_bucket: MockRmBucket) -> Replication {
        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let settings = ReplicationSettings {
            name: "test".to_string(),
            src_bucket: "src".to_string(),
            dst_bucket: "remote".to_string(),
            dst_host: "http://localhost:8383".to_string(),
            dst_token: "token".to_string(),
            entries: vec![],
            include: Labels::new(),
            exclude: Labels::new(),
        };

        let filter = TransactionFilter::new(
            settings.src_bucket.clone(),
            settings.entries.clone(),
            settings.include.clone(),
            settings.exclude.clone(),
        );

        let storage = Arc::new(RwLock::new(Storage::new(tmp_dir)));

        {
            let mut lock = storage.write().await;

            let bucket = lock
                .create_bucket("src", BucketSettings::default())
                .unwrap();

            let tx = bucket
                .write_record("test", 10, 4, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();
            tx.send(Ok(Some(Bytes::from("test")))).await.unwrap();
            tx.send(Ok(None)).await.unwrap();
        }

        Replication::build(
            ReplicationComponents {
                remote_bucket: Box::new(remote_bucket),
                filter,
                storage,
            },
            settings,
        )
    }

    async fn transaction_log_is_empty(replication: Replication) -> bool {
        sleep(std::time::Duration::from_millis(100)).await;

        replication
            .log_map
            .read()
            .await
            .get("test")
            .unwrap()
            .read()
            .await
            .is_empty()
    }
}
