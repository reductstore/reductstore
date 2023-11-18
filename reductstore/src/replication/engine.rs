// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::cfg::ReplicationCfg;
use crate::replication::{ReplicationEngine, ReplicationNotification};
use async_trait::async_trait;
use log::info;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

pub(crate) struct ReplicationEngineImpl {
    replications: HashMap<String, ReplicationCfg>,
    tx: Sender<ReplicationNotification>,
}

#[async_trait]
impl ReplicationEngine for ReplicationEngineImpl {
    fn add_replication(&mut self, cfg: ReplicationCfg) -> Result<(), ReductError> {
        self.replications.insert(cfg.name.clone(), cfg);
        Ok(())
    }

    async fn notify(&self, notification: ReplicationNotification) -> Result<(), ReductError> {
        self.tx.send(notification).await.map_err(|_| {
            ReductError::internal_server_error("Failed to send replication notification")
        })
    }
}

impl ReplicationEngineImpl {
    pub(crate) fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(notification) = rx.recv().await {
                info!("Replication notification: {:?}", notification);
            }
        });
        Self {
            replications: HashMap::new(),
            tx,
        }
    }
}
