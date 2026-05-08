// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::AuditEvent;
use crate::lifecycle::action::{LifecycleAction, LifecycleContext};
use crate::lifecycle::audit::LifecycleAuditPayload;

use crate::lifecycle::LifecycleAuditSink;
use log::{debug, error, info};
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{
    LifecycleInfo, LifecycleMode, LifecycleSettings, LifecycleType,
};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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
    mode: Arc<AtomicU8>,
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
        let mode = settings.mode;
        Self {
            name,
            is_provisioned: false,
            settings,
            interval,
            action,
            context,
            audit_sink,
            stop_flag: Arc::new(AtomicBool::new(false)),
            mode: Arc::new(AtomicU8::new(mode as u8)),
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
        let mode = Arc::clone(&self.mode);

        let handle = tokio::spawn(async move {
            debug!("Lifecycle worker '{}' started", name);
            while !stop_flag.load(Ordering::Relaxed) {
                if matches!(Self::load_mode_from(&mode), LifecycleMode::Disabled) {
                    Self::sleep_with_stop(interval, Arc::clone(&stop_flag)).await;
                    continue;
                }

                let started = std::time::Instant::now();
                match action.run(&name, &settings, context.clone()).await {
                    Ok(result) => {
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
            mode: self.load_mode(),
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

    pub(super) fn set_mode(&mut self, mode: LifecycleMode) {
        self.settings.mode = mode;
        self.mode.store(mode as u8, Ordering::Relaxed);
    }

    pub(super) fn is_running(&self) -> bool {
        self.worker_handle.is_some()
    }

    fn load_mode(&self) -> LifecycleMode {
        Self::load_mode_from(&self.mode)
    }

    fn load_mode_from(mode: &Arc<AtomicU8>) -> LifecycleMode {
        match mode.load(Ordering::Relaxed) {
            x if x == LifecycleMode::Disabled as u8 => LifecycleMode::Disabled,
            _ => LifecycleMode::Enabled,
        }
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
            debug!(
                "Skipping lifecycle audit event for '{}' because audit sink is not configured",
                name
            );
            return;
        };

        let (status, message, payload) = match result {
            Ok(processed_records) => (
                200u16,
                "".to_string(),
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
            entry_name: format!("__lifecycle_tasks/{}", name),
            status,
            message,
            payload,
        };

        info!(
            "Lifecycle audit event: policy='{}', action='{}', bucket='{}', status={}, message='{}', payload={}",
            name,
            format!("{:?}", action_type).to_lowercase(),
            bucket,
            status,
            event.message,
            event.payload
        );

        let audit_logger = Arc::clone(&sink.audit_logger);
        let lock = audit_logger.write().await;
        match lock {
            Ok(mut audit_logger) => {
                if let Err(err) = audit_logger.log_event(event).await {
                    error!(
                        "Failed to persist lifecycle audit event for '{}': {}",
                        name, err
                    );
                }
            }
            Err(err) => error!(
                "Failed to lock audit repository for lifecycle event '{}': {}",
                name, err
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditEvent, LogAuditEvent};
    use crate::cfg::Cfg;
    use crate::core::sync::AsyncRwLock;
    use crate::lifecycle::action::LifecycleRunResult;
    use crate::storage::engine::StorageEngine;
    use async_trait::async_trait;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::unprocessable_entity;
    use std::sync::Mutex;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    mockall::mock! {
        Action {}

        #[async_trait]
        impl LifecycleAction for Action {
            fn lifecycle_type(&self) -> LifecycleType;

            async fn run(
                &self,
                name: &str,
                settings: &LifecycleSettings,
                context: LifecycleContext,
            ) -> Result<LifecycleRunResult, ReductError>;
        }
    }

    #[tokio::test]
    async fn new_initializes_mode_from_settings() {
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(MockAction::new());
        let task = new_task(LifecycleMode::Disabled, action).await;

        assert_eq!(task.info().mode, LifecycleMode::Disabled);
    }

    #[tokio::test]
    async fn set_mode_updates_info() {
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(MockAction::new());
        let mut task = new_task(LifecycleMode::Enabled, action).await;

        task.set_mode(LifecycleMode::Disabled);
        assert_eq!(task.info().mode, LifecycleMode::Disabled);

        task.set_mode(LifecycleMode::Enabled);
        assert_eq!(task.info().mode, LifecycleMode::Enabled);
    }

    #[tokio::test]
    async fn set_mode_updates_settings() {
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(MockAction::new());
        let mut task = new_task(LifecycleMode::Enabled, action).await;

        task.set_mode(LifecycleMode::Disabled);

        assert_eq!(task.settings().mode, LifecycleMode::Disabled);
    }

    #[tokio::test]
    async fn info_reports_correct_state() {
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(MockAction::new());
        let mut task = new_task(LifecycleMode::Enabled, action).await;

        task.set_provisioned(true);
        task.start();

        let info = task.info();
        assert_eq!(info.name, "test");
        assert!(info.is_provisioned);
        assert!(info.is_running);
        assert_eq!(info.mode, LifecycleMode::Enabled);

        task.stop().await;
    }

    #[tokio::test]
    async fn worker_skips_action_when_disabled() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut action = MockAction::new();
        action
            .expect_lifecycle_type()
            .return_const(LifecycleType::Delete);
        action.expect_run().returning(move |name, _, _| {
            tx.send(name.to_string()).unwrap();
            Ok(LifecycleRunResult::default())
        });

        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(action);
        let mut task = new_task(LifecycleMode::Disabled, action).await;
        task.start();

        assert!(timeout(Duration::from_millis(250), rx.recv())
            .await
            .is_err());

        task.stop().await;
    }

    #[tokio::test]
    async fn worker_runs_action_when_enabled() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut action = MockAction::new();
        action
            .expect_lifecycle_type()
            .return_const(LifecycleType::Delete);
        action.expect_run().times(1).returning(move |name, _, _| {
            tx.send(name.to_string()).unwrap();
            Ok(LifecycleRunResult {
                affected_records: 1,
            })
        });

        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(action);
        let mut task = new_task(LifecycleMode::Enabled, action).await;
        task.start();

        let call = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(call, "test");

        task.stop().await;
    }

    #[derive(Clone)]
    struct CapturingAuditLogger {
        events: Arc<Mutex<Vec<AuditEvent>>>,
    }

    #[async_trait]
    impl LogAuditEvent for CapturingAuditLogger {
        async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn log_audit_event_writes_success_message() {
        let captured = Arc::new(Mutex::new(Vec::<AuditEvent>::new()));
        let sink = LifecycleAuditSink {
            audit_logger: Arc::new(AsyncRwLock::new(Box::new(CapturingAuditLogger {
                events: Arc::clone(&captured),
            })
                as Box<dyn LogAuditEvent + Send + Sync>)),
            instance_name: "instance-1".to_string(),
        };

        LifecycleTask::log_audit_event(
            Some(sink),
            "policy-1",
            LifecycleType::Delete,
            "bucket-1",
            0.25,
            Ok(42),
        )
        .await;

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1);
        let event = &events[0];

        assert_eq!(event.event_type, "lifecycle_run");
        assert_eq!(event.instance, "instance-1");
        assert_eq!(event.entry_name, "__lifecycle_tasks/policy-1");
        assert_eq!(event.status, 200);
        assert_eq!(event.message, "");
        assert_eq!(event.payload["policy_name"], "policy-1");
        assert_eq!(event.payload["action_type"], "delete");
        assert_eq!(event.payload["bucket"], "bucket-1");
        assert_eq!(event.payload["processed_records"], 42);
        assert!(event.payload.get("error_code").is_none());
        assert!(event.payload.get("error_message").is_none());
    }

    #[tokio::test]
    async fn log_audit_event_writes_error_message() {
        let captured = Arc::new(Mutex::new(Vec::<AuditEvent>::new()));
        let sink = LifecycleAuditSink {
            audit_logger: Arc::new(AsyncRwLock::new(Box::new(CapturingAuditLogger {
                events: Arc::clone(&captured),
            })
                as Box<dyn LogAuditEvent + Send + Sync>)),
            instance_name: "instance-1".to_string(),
        };

        let err = unprocessable_entity!("failed to run");
        LifecycleTask::log_audit_event(
            Some(sink),
            "policy-1",
            LifecycleType::Delete,
            "bucket-1",
            0.25,
            Err(err),
        )
        .await;

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1);
        let event = &events[0];

        assert_eq!(event.event_type, "lifecycle_run");
        assert_eq!(event.instance, "instance-1");
        assert_eq!(event.entry_name, "__lifecycle_tasks/policy-1");
        assert_eq!(event.status, 422);
        assert_eq!(event.message, "failed to run");
        assert_eq!(event.payload["policy_name"], "policy-1");
        assert_eq!(event.payload["action_type"], "delete");
        assert_eq!(event.payload["bucket"], "bucket-1");
        assert_eq!(event.payload["processed_records"], serde_json::Value::Null);
        assert_eq!(event.payload["error_code"], 422);
        assert_eq!(event.payload["error_message"], "failed to run");
    }

    async fn new_task(
        mode: LifecycleMode,
        action: Arc<dyn LifecycleAction + Send + Sync>,
    ) -> LifecycleTask {
        let settings = LifecycleSettings {
            lifecycle_type: LifecycleType::Delete,
            bucket: "bucket-1".to_string(),
            entries: vec!["entry-1".to_string()],
            max_age: "1d".to_string(),
            interval: "100ms".to_string(),
            when: None,
            mode,
        };

        LifecycleTask::new(
            "test".to_string(),
            settings,
            Duration::from_millis(100),
            action,
            LifecycleContext::new(storage().await),
            None,
        )
    }

    async fn storage() -> Arc<StorageEngine> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };

        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg)
            .build()
            .await;
        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();
        Arc::new(storage)
    }
}
