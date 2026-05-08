// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::LogAuditEvent;
use crate::core::sync::AsyncRwLock;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{
    FullLifecycleInfo, LifecycleInfo, LifecycleMode, LifecycleSettings,
};
use std::sync::Arc;

mod action;
pub(crate) mod audit;
mod lifecycle_repository;
mod lifecycle_task;

#[derive(Clone)]
pub struct LifecycleAuditSink {
    pub(crate) audit_logger: Arc<AsyncRwLock<Box<dyn LogAuditEvent + Send + Sync>>>,
    pub(crate) instance_name: String,
}

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

    /// Set lifecycle mode.
    async fn set_mode(&mut self, name: &str, mode: LifecycleMode) -> Result<(), ReductError>;

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
