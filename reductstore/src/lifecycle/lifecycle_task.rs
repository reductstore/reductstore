// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::lifecycle::action::{LifecycleAction, LifecycleContext};
use log::{debug, error};
use reduct_base::msg::lifecycle_api::{LifecycleInfo, LifecycleSettings};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

pub(super) struct LifecycleTask {
    name: String,
    is_provisioned: bool,
    settings: LifecycleSettings,
    interval: Duration,
    action: Arc<dyn LifecycleAction + Send + Sync>,
    context: LifecycleContext,
    stop_flag: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
}

impl LifecycleTask {
    pub(super) fn new(
        name: String,
        settings: LifecycleSettings,
        interval: Duration,
        action: Arc<dyn LifecycleAction + Send + Sync>,
        context: LifecycleContext,
    ) -> Self {
        Self {
            name,
            is_provisioned: false,
            settings,
            interval,
            action,
            context,
            stop_flag: Arc::new(AtomicBool::new(false)),
            worker_handle: None,
        }
    }

    pub(super) fn start(&mut self) {
        if self.is_running() {
            return;
        }

        self.stop_flag.store(false, Ordering::Relaxed);
        let name = self.name.clone();
        let interval = self.interval;
        let settings = self.settings.clone();
        let action = Arc::clone(&self.action);
        let context = self.context.clone();
        let stop_flag = Arc::clone(&self.stop_flag);

        let handle = tokio::spawn(async move {
            debug!("Lifecycle worker '{}' started", name);
            while !stop_flag.load(Ordering::Relaxed) {
                match action.run(&name, &settings, context.clone()).await {
                    Ok(result) => debug!(
                        "Lifecycle worker '{}' ran {:?} action, affected_records={}",
                        name,
                        action.lifecycle_type(),
                        result.affected_records
                    ),
                    Err(err) => error!("Lifecycle worker '{}' failed: {}", name, err),
                }
                Self::sleep_with_stop(interval, Arc::clone(&stop_flag)).await;
            }
            debug!("Lifecycle worker '{}' stopped", name);
        });

        self.worker_handle = Some(handle);
    }

    pub(super) async fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.worker_handle.take() {
            if let Err(err) = handle.await {
                error!("Lifecycle worker task failed to join: {:?}", err);
            }
        }
    }

    pub(super) fn info(&self) -> LifecycleInfo {
        LifecycleInfo {
            name: self.name.clone(),
            is_provisioned: self.is_provisioned,
            is_running: self.is_running(),
        }
    }

    pub(super) fn settings(&self) -> &LifecycleSettings {
        &self.settings
    }

    pub(super) fn is_provisioned(&self) -> bool {
        self.is_provisioned
    }

    pub(super) fn set_provisioned(&mut self, provisioned: bool) {
        self.is_provisioned = provisioned;
    }

    pub(super) fn is_running(&self) -> bool {
        self.worker_handle.is_some()
    }

    async fn sleep_with_stop(interval: Duration, stop_flag: Arc<AtomicBool>) {
        let sleep_step = Duration::from_millis(100);
        let mut slept = Duration::ZERO;
        while slept < interval && !stop_flag.load(Ordering::Relaxed) {
            let remaining = interval.saturating_sub(slept);
            let step = remaining.min(sleep_step);
            tokio::time::sleep(step).await;
            slept += step;
        }
    }
}

impl Drop for LifecycleTask {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.worker_handle.take() {
            tokio::spawn(async move {
                if let Err(err) = handle.await {
                    error!("Lifecycle worker task failed to join: {:?}", err);
                }
            });
        }
    }
}
