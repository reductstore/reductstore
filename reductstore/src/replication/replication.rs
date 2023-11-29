// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
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
}

impl Replication {
    pub fn new(storage_path: PathBuf, settings: ReplicationSettings) -> Self {
        let filter = TransactionFilter::new(
            settings.src_bucket.clone(),
            settings.entries.clone(),
            settings.include.clone(),
            settings.exclude.clone(),
        );

        let logs = Arc::new(RwLock::new(HashMap::<String, TransactionLog>::new()));

        let map_log = logs.clone();
        let config = settings.clone();

        let client = ReductClient::builder()
            .url(config.remote_host.as_str())
            .api_token(&config.remote_token)
            .build();

        let bucket = client.get_bucket(&config.remote_bucket);
        tokio::spawn(async move {
            loop {
                for (entry_name, log) in map_log.write().await.iter_mut() {
                    if log.is_empty() {
                        continue;
                    }

                    let transaction = log.front().await.unwrap().unwrap();

                    info!("Replicating transaction {}/{:?}", entry_name, transaction);
                    log.pop_front().await.unwrap();
                }
            }
        });

        Self {
            settings,
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
