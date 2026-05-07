// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{FullLifecycleInfo, LifecycleInfo, LifecycleSettings};

mod action;
mod lifecycle_repository;
mod lifecycle_task;

#[async_trait]
pub trait ManageLifecycles {
    /// Create a lifecycle policy.
    async fn create_lifecycle(
        &mut self,
        name: &str,
        settings: LifecycleSettings,
    ) -> Result<(), ReductError>;

    /// Update a lifecycle policy.
    async fn update_lifecycle(
        &mut self,
        name: &str,
        settings: LifecycleSettings,
    ) -> Result<(), ReductError>;

    /// List lifecycle policies.
    async fn lifecycles(&self) -> Result<Vec<LifecycleInfo>, ReductError>;

    /// Get lifecycle policy information.
    async fn get_info(&self, name: &str) -> Result<FullLifecycleInfo, ReductError>;

    /// Get lifecycle policy settings.
    async fn get_lifecycle_settings(&self, name: &str) -> Result<LifecycleSettings, ReductError>;

    /// Check if lifecycle worker is running.
    async fn is_lifecycle_running(&self, name: &str) -> Result<bool, ReductError>;

    /// Mark lifecycle policy as provisioned/unprovisioned.
    async fn set_lifecycle_provisioned(
        &mut self,
        name: &str,
        provisioned: bool,
    ) -> Result<(), ReductError>;

    /// Remove a lifecycle policy.
    async fn remove_lifecycle(&mut self, name: &str) -> Result<(), ReductError>;

    /// Start background workers if they are not running yet.
    fn start(&mut self);

    /// Stop background workers and wait until they finish.
    async fn stop(&mut self);
}

pub(crate) use lifecycle_repository::LifecycleRepoBuilder;
