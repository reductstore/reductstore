// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::replication::Replication;
use crate::replication::{ReplicationEngine, ReplicationNotification};
use async_trait::async_trait;
use log::info;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

pub(crate) struct ReplicationEngineImpl {
    replications: HashMap<String, Replication>,
}

#[async_trait]
impl ReplicationEngine for ReplicationEngineImpl {
    fn add_replication(&mut self, cfg: Replication) -> Result<(), ReductError> {
        self.replications.insert(cfg.name().clone(), cfg);
        Ok(())
    }

    async fn notify(&self, notification: ReplicationNotification) -> Result<(), ReductError> {
        for (_, replication) in self.replications.iter() {
            let _ = replication.notify(notification.clone()).await?;
        }
        Ok(())
    }
}

impl ReplicationEngineImpl {
    pub(crate) fn new() -> Self {
        Self {
            replications: HashMap::new(),
        }
    }
}
