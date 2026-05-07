// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::audit::LifecycleAuditPayload;
use crate::audit::AuditEvent;
use crate::lifecycle::action::{LifecycleAction, LifecycleContext};
use crate::lifecycle::LifecycleAuditSink;
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::LifecycleType;
use reduct_base::msg::lifecycle_api::{LifecycleInfo, LifecycleSettings};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

pub(super) struct LifecycleTask {
    name: String,
    is_provisioned: bool,
    settings: LifecycleSettings,
    interval: Duration,
    action: Arc<dyn LifecycleAction + Send + Sync>,
    context: LifecycleContext,
    audit_sink: Option<LifecycleAuditSink>,
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
        audit_sink: Option<LifecycleAuditSink>,
    ) -> Self {
        Self {
            name,
            is_provisioned: false,
            settings,
            interval,
            action,
            context,
            audit_sink,
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
        let audit_sink = self.audit_sink.clone();
        let stop_flag = Arc::clone(&self.stop_flag);

        let handle = tokio::spawn(async move {
            debug!("Lifecycle worker '{}' started", name);
            while !stop_flag.load(Ordering::Relaxed) {
                let started = std::time::Instant::now();
                match action.run(&name, &settings, context.clone()).await {
                    Ok(result) => {
                        debug!(
                            "Lifecycle worker '{}' ran {:?} action, affected_records={}",
                            name,
                            action.lifecycle_type(),
                            result.affected_records
                        );
                        Self::log_audit_event(
                            audit_sink.clone(),
                            &name,
                            action.lifecycle_type(),
                            &settings.bucket,
                            started.elapsed().as_secs_f64(),
                            Ok(result.affected_records),
                        )
                        .await;
                    }
                    Err(err) => {
                        error!("Lifecycle worker '{}' failed: {}", name, err);
                        Self::log_audit_event(
                            audit_sink.clone(),
                            &name,
                            action.lifecycle_type(),
                            &settings.bucket,
                            started.elapsed().as_secs_f64(),
                            Err(err),
                        )
                        .await;
                    }
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

    async fn log_audit_event(
        audit_sink: Option<LifecycleAuditSink>,
        name: &str,
        action_type: LifecycleType,
        bucket: &str,
        duration: f64,
        result: Result<u64, ReductError>,
    ) {
        let Some(sink) = audit_sink else {
            return;
        };

        let (status, message, payload) = match result {
            Ok(processed_records) => (
                200u16,
                format!("Lifecycle '{}' completed", name),
                LifecycleAuditPayload::success(
                    name,
                    &format!("{:?}", action_type).to_lowercase(),
                    bucket,
                    duration,
                    processed_records,
                )
                .to_value(),
            ),
            Err(err) => (
                err.status as u16,
                err.message.clone(),
                LifecycleAuditPayload::error(
                    name,
                    &format!("{:?}", action_type).to_lowercase(),
                    bucket,
                    duration,
                    err.status as u16,
                    &err.message,
                )
                .to_value(),
            ),
        };

        let event = AuditEvent {
            event_type: "lifecycle_run".to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            instance: sink.instance_name.clone(),
            token_name: "system:lifecycle".to_string(),
            method: "".to_string(),
            path: "".to_string(),
            status,
            message,
            client_ip: None,
            call_count: 1,
            duration,
            payload: Some(payload),
        };

        let audit_logger = Arc::clone(&sink.audit_logger);
        let lock = audit_logger.write().await;
        match lock {
            Ok(mut audit_logger) => {
                if let Err(err) = audit_logger.log_event(event).await {
                    debug!("Failed to persist lifecycle audit event: {}", err);
                }
            }
            Err(err) => debug!(
                "Failed to lock audit repository for lifecycle event: {}",
                err
            ),
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
