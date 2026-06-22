// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{LifecycleSettings, LifecycleType};
use std::sync::Arc;
mod compress;
mod delete;
use compress::CompressLifecycleAction;
use delete::DeleteLifecycleAction;

use crate::storage::engine::StorageEngine;

#[derive(Clone)]
pub(super) struct LifecycleContext {
    pub(super) storage: Arc<StorageEngine>,
}

impl LifecycleContext {
    pub(super) fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct LifecycleRunResult {
    pub(super) affected_records: u64,
    pub(super) affected_blocks: Option<u64>,
}

#[async_trait]
pub(super) trait LifecycleAction {
    fn lifecycle_type(&self) -> LifecycleType;

    async fn run(
        &self,
        name: &str,
        settings: &LifecycleSettings,
        context: LifecycleContext,
    ) -> Result<LifecycleRunResult, ReductError>;
}

pub(super) fn build_lifecycle_action(
    lifecycle_type: LifecycleType,
) -> Arc<dyn LifecycleAction + Send + Sync> {
    match lifecycle_type {
        LifecycleType::Delete => Arc::new(DeleteLifecycleAction),
        LifecycleType::Compress => Arc::new(CompressLifecycleAction),
    }
}
