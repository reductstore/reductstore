// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use log::info;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use url::Url;

pub struct Replication {
    name: String,
    src_bucket: String,
    remote_bucket: String,
    remote_host: Url,
    remote_token: String,
    entries: Vec<String>,
    include: Labels,
    exclude: Labels,
    tx: Sender<TransactionNotification>,
}

impl Replication {
    pub fn new(
        name: String,
        src_bucket: String,
        remote_bucket: String,
        remote_host: Url,
        remote_token: String,
        entries: Vec<String>,
        include: Labels,
        exclude: Labels,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<TransactionNotification>(100);
        tokio::spawn(async move {
            while let Some(notification) = rx.recv().await {
                // TODO: filter by entries and labels

                info!("Replication notification: {:?}", notification);
            }
        });

        Self {
            name,
            src_bucket,
            remote_bucket,
            remote_host,
            remote_token,
            entries,
            include,
            exclude,
            tx,
        }
    }

    pub async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError> {
        if notification.bucket != self.src_bucket {
            return Ok(());
        }
        self.tx.send(notification).await.map_err(|_| {
            ReductError::internal_server_error("Failed to send replication notification")
        })
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}
