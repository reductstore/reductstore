// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::aggregator::{AuditAggregator, FlushHandler};
use crate::audit::{AuditEvent, ManageAudit, AUDIT_BUCKET_NAME};
use crate::cfg::Cfg;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::ErrorCode;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
use reduct_base::Labels;
use std::sync::Arc;

// The local audit repository writes audit events into the internal $audit bucket.
pub(crate) struct AuditRepository {
    aggregator: AuditAggregator,
    #[cfg(test)]
    storage: Arc<StorageEngine>,
}

impl AuditRepository {
    pub async fn new(cfg: Cfg, storage: Arc<StorageEngine>) -> Self {
        #[cfg(test)]
        let test_storage = Arc::clone(&storage);
        let sink_storage = Arc::clone(&storage);
        let audit_settings = Self::bucket_settings(&cfg);
        let handler: FlushHandler = Arc::new(move |event| {
            let storage = Arc::clone(&sink_storage);
            let settings = audit_settings.clone();
            Box::pin(async move { Self::write_event_to_bucket(storage, settings, event).await })
        });

        let aggregator = AuditAggregator::new(handler);

        Self {
            aggregator,
            #[cfg(test)]
            storage: test_storage,
        }
    }

    fn bucket_settings(cfg: &Cfg) -> BucketSettings {
        if let Some(quota_size) = cfg.audit_conf.quota_size {
            BucketSettings {
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(quota_size),
                ..BucketSettings::default()
            }
        } else {
            BucketSettings::default()
        }
    }

    async fn write_event_to_bucket(
        storage: Arc<StorageEngine>,
        bucket_settings: BucketSettings,
        event: AuditEvent,
    ) -> Result<(), ReductError> {
        let instance = if event.instance.is_empty() {
            "unknown".to_string()
        } else {
            event.instance.clone()
        };
        let entry_name = format!("{}/{}", instance, event.token_name);
        let labels = Labels::from([("status".to_string(), event.status.to_string())]);
        let payload = serde_json::to_vec(&event)
            .map_err(|err| internal_server_error!("Failed to serialize audit event: {}", err))?;
        let mut writer = match storage
            .begin_write(
                AUDIT_BUCKET_NAME,
                &entry_name,
                event.timestamp,
                payload.len() as u64,
                "application/json".to_string(),
                labels.clone(),
            )
            .await
        {
            Ok(writer) => writer,
            Err(err) if err.status == ErrorCode::NotFound => {
                storage
                    .create_system_bucket(AUDIT_BUCKET_NAME, bucket_settings)
                    .await?;
                storage
                    .begin_write(
                        AUDIT_BUCKET_NAME,
                        &entry_name,
                        event.timestamp,
                        payload.len() as u64,
                        "application/json".to_string(),
                        labels,
                    )
                    .await?
            }
            Err(err) => return Err(err),
        };
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
        Ok(())
    }
}

#[async_trait]
impl ManageAudit for AuditRepository {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError> {
        self.aggregator.log_event(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::aggregator::AGGREGATION_WINDOW_SECS;
    use crate::cfg::Cfg;
    use crate::storage::engine::StorageEngine;
    use reduct_base::io::ReadRecord;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    #[fixture]
    async fn repo() -> AuditRepository {
        let tmp_dir = tempdir().unwrap();
        let mut cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        cfg.audit_conf.enabled = true;
        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg.clone())
            .build()
            .await;

        AuditRepository::new(cfg, Arc::new(storage)).await
    }

    fn make_event(
        token_name: &str,
        endpoint: &str,
        status: u16,
        message: &str,
        timestamp: u64,
    ) -> AuditEvent {
        AuditEvent {
            timestamp,
            instance: "unknown".to_string(),
            token_name: token_name.to_string(),
            endpoint: endpoint.to_string(),
            status,
            message: message.to_string(),
            call_count: 1,
            duration: 0.1,
        }
    }

    fn audit_entry_name(token_name: &str) -> String {
        format!("unknown/{}", token_name)
    }

    async fn read_audit_event(
        repo: &AuditRepository,
        token_name: &str,
        timestamp: u64,
    ) -> AuditEvent {
        let bucket = repo
            .storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket
            .begin_read(&audit_entry_name(token_name), timestamp)
            .await
            .unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        serde_json::from_slice(&record).unwrap()
    }

    async fn read_audit_labels(repo: &AuditRepository, token_name: &str, timestamp: u64) -> Labels {
        let bucket = repo
            .storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let reader = bucket
            .begin_read(&audit_entry_name(token_name), timestamp)
            .await
            .unwrap();
        reader.meta().labels().clone()
    }

    async fn audit_record_exists(repo: &AuditRepository, token_name: &str, timestamp: u64) -> bool {
        let bucket = match repo.storage.get_bucket(AUDIT_BUCKET_NAME).await {
            Ok(bucket) => bucket.upgrade_and_unwrap(),
            Err(_) => return false,
        };

        bucket
            .begin_read(&audit_entry_name(token_name), timestamp)
            .await
            .is_ok()
    }

    async fn wait_for_aggregate_count(repo: &AuditRepository, expected: usize) {
        for _ in 0..50 {
            let state = repo.aggregator.state.read().await.unwrap();
            if state.aggregates.len() == expected {
                return;
            }
            drop(state);
            sleep(Duration::from_millis(20)).await;
        }

        let state = repo.aggregator.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), expected);
    }

    #[rstest]
    #[tokio::test]
    async fn aggregates_events_with_same_key(#[future] repo: AuditRepository) {
        let mut repo = repo.await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 2))
            .await
            .unwrap();

        wait_for_aggregate_count(&repo, 1).await;
        let state = repo.aggregator.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), 1);
        let aggregate = state.aggregates.values().next().unwrap();
        assert_eq!(aggregate.call_count, 2);
        assert!((aggregate.total_duration - 0.2).abs() < 1e-9);
        assert_eq!(aggregate.first_timestamp, 1);
        assert_eq!(aggregate.last_timestamp, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn separates_events_with_different_status(#[future] repo: AuditRepository) {
        let mut repo = repo.await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();
        repo.log_event(make_event(
            "token-1",
            "GET /api/v1/b/test",
            403,
            "forbidden",
            2,
        ))
        .await
        .unwrap();

        wait_for_aggregate_count(&repo, 2).await;
        let state = repo.aggregator.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), 2);
    }

    #[rstest]
    #[tokio::test]
    async fn separates_events_with_different_message(#[future] repo: AuditRepository) {
        let mut repo = repo.await;

        repo.log_event(make_event(
            "token-1",
            "GET /api/v1/b/test",
            500,
            "disk full",
            1,
        ))
        .await
        .unwrap();
        repo.log_event(make_event(
            "token-1",
            "GET /api/v1/b/test",
            500,
            "timeout",
            2,
        ))
        .await
        .unwrap();

        wait_for_aggregate_count(&repo, 2).await;
        let state = repo.aggregator.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), 2);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn flushes_single_event_after_delay(#[future] repo: AuditRepository) {
        let mut repo = repo.await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();

        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.token_name, "token-1");
        assert_eq!(event.endpoint, "GET /api/v1/b/test");
        assert_eq!(event.status, 200);
        assert_eq!(event.message, "");
        assert_eq!(event.call_count, 1);
        assert!((event.duration - 0.1).abs() < 1e-9);
        assert_eq!(event.timestamp, 1);
        assert_eq!(event.instance, "unknown");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn flushes_status_label(#[future] repo: AuditRepository) {
        let mut repo = repo.await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();

        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        let labels = read_audit_labels(&repo, "token-1", 1).await;
        assert_eq!(
            labels,
            Labels::from([("status".to_string(), "200".to_string())])
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn creates_system_bucket_when_missing(#[future] repo: AuditRepository) {
        let repo = repo.await;

        AuditRepository::write_event_to_bucket(
            Arc::clone(&repo.storage),
            BucketSettings::default(),
            make_event("token-1", "GET /api/v1/b/test", 200, "", 1),
        )
        .await
        .unwrap();

        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.token_name, "token-1");
        assert_eq!(event.status, 200);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn stores_event_under_unknown_instance_when_instance_is_empty(
        #[future] repo: AuditRepository,
    ) {
        let repo = repo.await;
        let mut event = make_event("token-empty", "GET /api/v1/b/test", 200, "", 11);
        event.instance = "".to_string();

        AuditRepository::write_event_to_bucket(
            Arc::clone(&repo.storage),
            BucketSettings::default(),
            event,
        )
        .await
        .unwrap();

        let event = read_audit_event(&repo, "token-empty", 11).await;
        assert_eq!(event.token_name, "token-empty");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn creates_audit_bucket_with_fifo_quota_when_configured() {
        let tmp_dir = tempdir().unwrap();
        let mut cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        cfg.audit_conf.enabled = true;
        cfg.audit_conf.quota_size = Some(1024);

        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );

        let mut repo = AuditRepository::new(cfg, Arc::clone(&storage)).await;
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();

        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        let bucket = storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let settings = bucket.settings().await.unwrap();
        assert_eq!(settings.quota_type, Some(QuotaType::FIFO));
        assert_eq!(settings.quota_size, Some(1024));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn second_matching_event_delays_flush_and_aggregates_record(
        #[future] repo: AuditRepository,
    ) {
        let mut repo = repo.await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();
        sleep(Duration::from_millis((AGGREGATION_WINDOW_SECS * 1000) / 2)).await;
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 2))
            .await
            .unwrap();

        sleep(Duration::from_millis((AGGREGATION_WINDOW_SECS * 1000) / 2)).await;
        assert!(!audit_record_exists(&repo, "token-1", 1).await);

        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;
        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.call_count, 2);
        assert!((event.duration - 0.2).abs() < 1e-9);
        assert_eq!(event.timestamp, 1);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn stale_timer_does_not_flush_after_version_change(#[future] repo: AuditRepository) {
        let mut repo = repo.await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 1))
            .await
            .unwrap();
        sleep(Duration::from_millis((AGGREGATION_WINDOW_SECS * 1000) / 2)).await;
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, "", 3))
            .await
            .unwrap();

        sleep(Duration::from_millis((AGGREGATION_WINDOW_SECS * 1000) / 2)).await;
        assert!(!audit_record_exists(&repo, "token-1", 1).await);

        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;
        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.call_count, 2);
        assert!((event.duration - 0.2).abs() < 1e-9);
        assert_eq!(event.timestamp, 1);
    }
}
