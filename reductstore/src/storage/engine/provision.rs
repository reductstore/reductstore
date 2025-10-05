// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::engine::StorageEngine;
use log::{error, info};
use reduct_base::error::ErrorCode;

impl StorageEngine {
    pub(super) fn provision_buckets(&self) {
        for (name, settings) in &self.cfg.buckets {
            let settings = match self.create_bucket(&name, settings.clone()) {
                Ok(bucket) => {
                    let bucket = bucket.upgrade().unwrap();
                    bucket.set_provisioned(true);
                    Ok(bucket.settings().clone())
                }
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        let bucket = self.get_bucket(&name).unwrap().upgrade().unwrap();
                        bucket.set_provisioned(false);
                        bucket.set_settings(settings.clone()).wait().unwrap();
                        bucket.set_provisioned(true);

                        Ok(bucket.settings().clone())
                    } else {
                        Err(e)
                    }
                }
            };

            if let Ok(settings) = settings {
                info!("Provisioned bucket '{}' with: {:?}", name, settings);
            } else {
                error!(
                    "Failed to provision bucket '{}': {}",
                    name,
                    settings.err().unwrap()
                );
            }
        }
    }
}
