// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{CfgParser, ExtCfgBounds, ProvisionedLifecycle};
use crate::core::env::{Env, GetEnv};
use crate::lifecycle::{LifecycleRepoBuilder, ManageLifecycles};
use crate::storage::engine::StorageEngine;
use log::{error, info};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::lifecycle_api::{LifecycleSettings, LifecycleType};
use std::collections::HashMap;
use std::sync::Arc;

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(in crate::cfg) async fn provision_lifecycle_repo(
        &self,
        storage: Arc<StorageEngine>,
    ) -> Result<Box<dyn ManageLifecycles + Send + Sync>, ReductError> {
        let mut repo = LifecycleRepoBuilder::new(self.cfg.clone())
            .build(Arc::clone(&storage))
            .await;

        for (name, lifecycle) in &self.cfg.lifecycles {
            if let Err(err) = repo
                .create_lifecycle(name, lifecycle.settings.clone())
                .await
            {
                if err.status() == ErrorCode::Conflict {
                    repo.update_lifecycle(name, lifecycle.settings.clone())
                        .await?;
                } else {
                    error!("Failed to provision lifecycle '{}': {}", name, err);
                    continue;
                }
            }

            repo.set_lifecycle_provisioned(name, true).await?;
            let info_data = repo.get_info(name).await?;
            info!(
                "Provisioned lifecycle '{}' with {:?}",
                name, info_data.settings
            );
        }

        Ok(repo)
    }

    pub(in crate::cfg) fn parse_lifecycles(
        env: &mut Env<EnvGetter>,
    ) -> HashMap<String, ProvisionedLifecycle> {
        let mut lifecycles = HashMap::<String, (String, ProvisionedLifecycle)>::new();
        for (id, name) in env.matches("RS_LIFECYCLE_(.*)_NAME") {
            lifecycles.insert(
                id,
                (
                    name,
                    ProvisionedLifecycle {
                        settings: LifecycleSettings::default(),
                    },
                ),
            );
        }

        let mut unfinished_lifecycles = vec![];
        for (id, (name, lifecycle)) in &mut lifecycles {
            if let Some(lifecycle_type) =
                env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_TYPE", id))
            {
                match lifecycle_type.to_lowercase().as_str() {
                    "delete" => lifecycle.settings.lifecycle_type = LifecycleType::Delete,
                    _ => {
                        error!(
                            "Lifecycle '{}' has invalid type '{}'. Drop it.",
                            name, lifecycle_type
                        );
                        unfinished_lifecycles.push(id.clone());
                        continue;
                    }
                }
            }

            if let Some(bucket) = env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_BUCKET", id))
            {
                lifecycle.settings.bucket = bucket;
            } else {
                error!("Lifecycle '{}' has no bucket. Drop it.", name);
                unfinished_lifecycles.push(id.clone());
                continue;
            }

            if let Some(max_age) =
                env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_MAX_AGE", id))
            {
                lifecycle.settings.max_age = max_age;
            } else {
                error!("Lifecycle '{}' has no max age. Drop it.", name);
                unfinished_lifecycles.push(id.clone());
                continue;
            }

            if let Some(entries) =
                env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_ENTRIES", id))
            {
                lifecycle.settings.entries = entries
                    .split(',')
                    .map(|entry| entry.trim())
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| entry.to_string())
                    .collect();
            }

            if let Some(when) =
                env.get_optional::<serde_json::Value>(&format!("RS_LIFECYCLE_{}_WHEN", id))
            {
                lifecycle.settings.when = Some(when);
            }
        }

        lifecycles
            .into_iter()
            .filter(|(id, _)| !unfinished_lifecycles.contains(id))
            .map(|(_, (name, lifecycle))| (name, lifecycle))
            .collect()
    }
}
