// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use url::Url;

pub struct ReplicationSettings {
    name: String,
    src_bucket: String,
    remote_bucket: String,
    remote_host: Url,
    remote_token: String,
    entries: Vec<String>,
    include: Labels,
    exclude: Labels,
}

pub struct Replication {
    settings: ReplicationSettings,
    filter: TransactionFilter,
    log: Arc<RwLock<TransactionLog>>,
}

impl Replication {
    pub async fn new(
        storage_path: PathBuf,
        settings: ReplicationSettings,
    ) -> Result<Self, ReductError> {
        let filter = TransactionFilter::new(
            settings.src_bucket.clone(),
            settings.entries.clone(),
            settings.include.clone(),
            settings.exclude.clone(),
        );

        let log = Arc::new(RwLock::new(
            TransactionLog::try_load_or_create(
                storage_path.join(format!("{}.log", settings.name)),
                1_000_000,
            )
            .await?,
        ));

        let log_clone = log.clone();
        tokio::spawn(async move {
            loop {
                if log_clone.read().await.is_empty() {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    continue;
                }

                let transaction = log_clone.write().await.pop_front().await.unwrap();
                info!("Replicating transaction: {:?}", transaction)
            }
        });

        Ok(Self {
            settings,
            filter,
            log,
        })
    }

    pub async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError> {
        if !self.filter.filter(&notification) {
            return Ok(());
        }

        if let Some(_) = self.log.write().await.push_back(notification.event).await? {
            error!("Transaction log is full, dropping the oldest transaction without replication");
        }

        Ok(())
    }

    pub fn name(&self) -> &String {
        &self.settings.name
    }
}
