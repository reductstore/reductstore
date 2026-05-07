// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::lifecycle::ManageLifecycles;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::forbidden;
use reduct_base::msg::lifecycle_api::{FullLifecycleInfo, LifecycleInfo, LifecycleSettings};

pub(super) struct ReadOnlyLifecycleRepository;

impl ReadOnlyLifecycleRepository {
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ManageLifecycles for ReadOnlyLifecycleRepository {
    async fn create_lifecycle(
        &mut self,
        _name: &str,
        _settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot create lifecycle in read-only mode"))
    }

    async fn update_lifecycle(
        &mut self,
        _name: &str,
        _settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot update lifecycle in read-only mode"))
    }

    async fn lifecycles(&self) -> Result<Vec<LifecycleInfo>, ReductError> {
        Ok(vec![])
    }

    async fn get_info(&self, _name: &str) -> Result<FullLifecycleInfo, ReductError> {
        Err(forbidden!("Cannot get lifecycle info in read-only mode"))
    }

    async fn get_lifecycle_settings(&self, _name: &str) -> Result<LifecycleSettings, ReductError> {
        Err(forbidden!(
            "Cannot get lifecycle settings in read-only mode"
        ))
    }

    async fn is_lifecycle_running(&self, _name: &str) -> Result<bool, ReductError> {
        Err(forbidden!("Cannot get lifecycle in read-only mode"))
    }

    async fn set_lifecycle_provisioned(
        &mut self,
        _name: &str,
        _provisioned: bool,
    ) -> Result<(), ReductError> {
        Err(forbidden!(
            "Cannot set lifecycle provisioned state in read-only mode"
        ))
    }

    async fn remove_lifecycle(&mut self, _name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot remove lifecycle in read-only mode"))
    }

    fn start(&mut self) {
        // No-op
    }

    async fn stop(&mut self) {
        // No-op
    }
}
