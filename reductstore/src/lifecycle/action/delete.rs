// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::duration::parse_duration_to_micros;
use crate::lifecycle::action::{LifecycleAction, LifecycleContext, LifecycleRunResult};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::{QueryEntry, QueryType};
use reduct_base::msg::lifecycle_api::{LifecycleSettings, LifecycleType};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct DeleteLifecycleAction;

#[async_trait]
impl LifecycleAction for DeleteLifecycleAction {
    fn lifecycle_type(&self) -> LifecycleType {
        LifecycleType::Delete
    }

    async fn run(
        &self,
        _name: &str,
        settings: &LifecycleSettings,
        context: LifecycleContext,
    ) -> Result<LifecycleRunResult, ReductError> {
        let max_age_us = parse_duration_to_micros(&settings.max_age)?;
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let cutoff = now_us.saturating_sub(max_age_us.max(0) as u64);
        let stop = Some(cutoff.saturating_add(1));

        let entries = if settings.entries.is_empty() {
            None
        } else {
            Some(settings.entries.clone())
        };

        let bucket = context
            .storage
            .get_bucket(&settings.bucket)
            .await?
            .upgrade()?;
        let affected_records = bucket
            .query_remove_records(QueryEntry {
                query_type: QueryType::Remove,
                entries,
                start: None,
                stop,
                when: settings.when.clone(),
                ..Default::default()
            })
            .await?;

        Ok(LifecycleRunResult { affected_records })
    }
}
