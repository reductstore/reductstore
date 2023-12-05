// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::RemoteBucketImpl;
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use crate::storage::storage::Storage;
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::Labels;
use reduct_rs::ReductClient;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
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
    log_map: Arc<RwLock<HashMap<String, TransactionLog>>>,
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

        let logs = Arc::new(RwLock::new(HashMap::<String, TransactionLog>::new()));

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
                for (entry_name, log) in map_log.write().await.iter_mut() {
                    if log.is_empty() {
                        continue;
                    }

                    let transaction = log.front().await.unwrap().unwrap();

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
                            error!("Failed to replicate transaction: {:?}", err);
                            break;
                        }
                    };

                    match remote_bucket.write_record(entry_name, read_record).await {
                        Ok(_) => {
                            log.pop_front().await.unwrap();
                        }
                        Err(err) => {
                            error!("Failed to replicate transaction: {:?}", err);
                            break;
                        }
                    }
                    log.pop_front().await.unwrap();
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
        let mut lock = self.log_map.write().await;
        let log = match lock.entry(notification.entry.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let log = TransactionLog::try_load_or_create(
                    self.storage_path.join(format!(
                        "{}/{}/transactions.log",
                        notification.bucket, notification.entry
                    )),
                    1000_000,
                )
                .await?;
                entry.insert(log)
            }
        };

        if let Some(_) = log.push_back(notification.event).await? {
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
}
