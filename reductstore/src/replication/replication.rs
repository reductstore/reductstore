// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::RemoteBucketImpl;
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use crate::storage::storage::Storage;
use hermit_abi::addrinfo;
use log::{debug, error, info};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::Labels;
use reduct_rs::ReductClient;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::sleep;
use url::Url;

#[derive(Debug, Clone)]
pub struct ReplicationSettings {
    pub name: String,
    pub src_bucket: String,
    pub remote_bucket: String,
    pub remote_host: Url,
    pub remote_token: String,
    pub entries: Vec<String>,
    pub include: Labels,
    pub exclude: Labels,
}

pub struct Replication {
    settings: ReplicationSettings,
    filter: TransactionFilter,
    storage_path: PathBuf,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<RwLock<Storage>>,
}

impl Replication {
    pub fn new(
        storage_path: PathBuf,
        storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) -> Self {
        let filter = TransactionFilter::new(
            settings.src_bucket.clone(),
            settings.entries.clone(),
            settings.include.clone(),
            settings.exclude.clone(),
        );

        let logs = Arc::new(RwLock::new(HashMap::<String, RwLock<TransactionLog>>::new()));

        let map_log = logs.clone();
        let config = settings.clone();

        let mut remote_bucket = RemoteBucketImpl::new(
            config.remote_host.clone(),
            config.remote_bucket.as_str(),
            config.remote_token.as_str(),
        );

        let local_storage = Arc::clone(&storage);
        tokio::spawn(async move {
            loop {
                for (entry_name, log) in map_log.read().await.iter() {
                    if log.read().await.is_empty() {
                        sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }

                    let transaction = log.write().await.front().await.unwrap().unwrap();

                    info!("Replicating transaction {}/{:?}", entry_name, transaction);

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
            }
        });

        Self {
            settings,
            storage,
            filter,
            storage_path,
            log_map: logs,
        }
    }

    pub async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError> {
        if !self.filter.filter(&notification) {
            return Ok(());
        }

        // find or create the transaction log for the entry
        info!(
            "find or create transaction {}/{:?}",
            notification.entry, notification.event
        );

        if !self.log_map.read().await.contains_key(&notification.entry) {
            let mut map = self.log_map.write().await;
            map.insert(
                notification.entry.clone(),
                RwLock::new(
                    TransactionLog::try_load_or_create(
                        self.storage_path.join(format!(
                            "{}/{}/{}.log",
                            notification.bucket, notification.entry, self.settings.name
                        )),
                        1000_000,
                    )
                    .await?,
                ),
            );
        };

        info!(
            "wait transaction {}/{:?}",
            notification.entry, notification.event
        );

        let log_map = self.log_map.read().await;
        let log = log_map.get(&notification.entry).unwrap();

        info!(
            "push transaction {}/{:?}",
            notification.entry, notification.event
        );
        if let Some(_) = log
            .write()
            .await
            .push_back(notification.event.clone())
            .await?
        {
            error!("Transaction log is full, dropping the oldest transaction without replication");
        }
        info!(
            "finish transaction {}/{:?}",
            notification.entry, notification.event
        );
        Ok(())
    }

    pub fn name(&self) -> &String {
        &self.settings.name
    }

    pub fn settings(&self) -> &ReplicationSettings {
        &self.settings
    }
}
