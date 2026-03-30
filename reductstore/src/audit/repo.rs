// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{AuditEvent, ManageAudit, AUDIT_BUCKET_NAME};
use crate::cfg::Cfg;
use crate::core::sync::AsyncRwLock;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

const AGGREGATION_WINDOW_SECS: u64 = 5;
const AUDIT_CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AuditAggregateKey {
    token_name: String,
    endpoint: String,
    status: u16,
}

#[derive(Debug, Clone)]
struct AuditAggregate {
    first_timestamp: u64,
    last_timestamp: u64,
    call_count: u64,
    total_duration: u64,
    flush_at: Instant,
}

#[derive(Default)]
struct AuditState {
    aggregates: HashMap<AuditAggregateKey, AuditAggregate>,
}

// The local audit repository writes audit events into the internal $audit bucket.
pub(crate) struct AuditRepository {
    storage: Arc<StorageEngine>,
    state: Arc<AsyncRwLock<AuditState>>,
    tx: mpsc::Sender<AuditEvent>,
}

impl AuditRepository {
    pub async fn new(_cfg: Cfg, storage: Arc<StorageEngine>) -> Self {
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(AUDIT_CHANNEL_SIZE);

        tokio::spawn(Self::run_worker(
            rx,
            Arc::clone(&state),
            Arc::clone(&storage),
        ));

        Self { storage, state, tx }
    }

    async fn aggregate_event(
        state: &Arc<AsyncRwLock<AuditState>>,
        event: AuditEvent,
    ) -> Result<(), ReductError> {
        let key = AuditAggregateKey {
            token_name: event.token_name,
            endpoint: event.endpoint,
            status: event.status,
        };

        let mut state = state.write().await?;
        let aggregate = state
            .aggregates
            .entry(key)
            .and_modify(|aggregate| {
                aggregate.call_count += event.call_count;
                aggregate.total_duration += event.duration;
                aggregate.last_timestamp = event.timestamp;
                if event.timestamp < aggregate.first_timestamp {
                    aggregate.first_timestamp = event.timestamp;
                }
                aggregate.flush_at = Instant::now() + Duration::from_secs(AGGREGATION_WINDOW_SECS);
            })
            .or_insert_with(|| AuditAggregate {
                first_timestamp: event.timestamp,
                last_timestamp: event.timestamp,
                call_count: event.call_count,
                total_duration: event.duration,
                flush_at: Instant::now() + Duration::from_secs(AGGREGATION_WINDOW_SECS),
            });

        let _ = aggregate;
        Ok(())
    }

    async fn run_worker(
        mut rx: mpsc::Receiver<AuditEvent>,
        state: Arc<AsyncRwLock<AuditState>>,
        storage: Arc<StorageEngine>,
    ) {
        loop {
            if let Some(deadline) = Self::next_deadline(&state).await {
                tokio::select! {
                    maybe_event = rx.recv() => {
                        match maybe_event {
                            Some(event) => {
                                let _ = Self::aggregate_event(&state, event).await;
                            }
                            None => {
                                let _ = Self::flush_all(&state, &storage).await;
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        let _ = Self::flush_expired(&state, &storage).await;
                    }
                }
            } else {
                match rx.recv().await {
                    Some(event) => {
                        let _ = Self::aggregate_event(&state, event).await;
                    }
                    None => break,
                }
            }
        }
    }

    async fn next_deadline(state: &Arc<AsyncRwLock<AuditState>>) -> Option<Instant> {
        let state = state.read().await.ok()?;
        state
            .aggregates
            .values()
            .map(|aggregate| aggregate.flush_at)
            .min()
    }

    async fn flush_expired(
        state: &Arc<AsyncRwLock<AuditState>>,
        storage: &Arc<StorageEngine>,
    ) -> Result<(), ReductError> {
        let now = Instant::now();
        let events = {
            let mut state = state.write().await?;
            let expired_keys: Vec<_> = state
                .aggregates
                .iter()
                .filter(|(_, aggregate)| aggregate.flush_at <= now)
                .map(|(key, _)| key.clone())
                .collect();

            expired_keys
                .into_iter()
                .filter_map(|key| {
                    state
                        .aggregates
                        .remove(&key)
                        .map(|aggregate| Self::into_event(key, aggregate))
                })
                .collect::<Vec<_>>()
        };

        for event in events {
            Self::write_event_to_bucket(Arc::clone(storage), event).await?;
        }

        Ok(())
    }

    async fn flush_all(
        state: &Arc<AsyncRwLock<AuditState>>,
        storage: &Arc<StorageEngine>,
    ) -> Result<(), ReductError> {
        let events = {
            let mut state = state.write().await?;
            state
                .aggregates
                .drain()
                .map(|(key, aggregate)| Self::into_event(key, aggregate))
                .collect::<Vec<_>>()
        };

        for event in events {
            Self::write_event_to_bucket(Arc::clone(storage), event).await?;
        }

        Ok(())
    }

    fn into_event(key: AuditAggregateKey, aggregate: AuditAggregate) -> AuditEvent {
        AuditEvent {
            timestamp: aggregate.first_timestamp,
            token_name: key.token_name,
            endpoint: key.endpoint,
            status: key.status,
            call_count: aggregate.call_count,
            duration: aggregate.total_duration,
        }
    }

    async fn write_event_to_bucket(
        storage: Arc<StorageEngine>,
        event: AuditEvent,
    ) -> Result<(), ReductError> {
        let labels = Labels::from([("status".to_string(), event.status.to_string())]);
        let payload = serde_json::to_vec(&event).map_err(|err| {
            ReductError::internal_server_error(&format!("Failed to serialize audit event: {}", err))
        })?;
        let mut writer = match storage
            .begin_write(
                AUDIT_BUCKET_NAME,
                &event.token_name,
                event.timestamp,
                payload.len() as u64,
                "application/json".to_string(),
                labels.clone(),
            )
            .await
        {
            Ok(writer) => writer,
            Err(_) => {
                storage
                    .create_system_bucket(
                        AUDIT_BUCKET_NAME,
                        reduct_base::msg::bucket_api::BucketSettings::default(),
                    )
                    .await?;
                storage
                    .begin_write(
                        AUDIT_BUCKET_NAME,
                        &event.token_name,
                        event.timestamp,
                        payload.len() as u64,
                        "application/json".to_string(),
                        labels,
                    )
                    .await?
            }
        };
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
        Ok(())
    }
}

#[async_trait]
impl ManageAudit for AuditRepository {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| ReductError::internal_server_error("Audit worker is not available"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::engine::StorageEngine;
    use reduct_base::io::ReadRecord;
    use tempfile::tempdir;
    use tokio::time::sleep;

    async fn create_repo() -> AuditRepository {
        let tmp_dir = tempdir().unwrap();
        let cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg.clone())
            .build()
            .await;

        AuditRepository::new(cfg, Arc::new(storage)).await
    }

    fn make_event(token_name: &str, endpoint: &str, status: u16, timestamp: u64) -> AuditEvent {
        AuditEvent {
            timestamp,
            token_name: token_name.to_string(),
            endpoint: endpoint.to_string(),
            status,
            call_count: 1,
            duration: 100,
        }
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
        let mut reader = bucket.begin_read(token_name, timestamp).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        serde_json::from_slice(&record).unwrap()
    }

    async fn audit_record_exists(repo: &AuditRepository, token_name: &str, timestamp: u64) -> bool {
        let bucket = match repo.storage.get_bucket(AUDIT_BUCKET_NAME).await {
            Ok(bucket) => bucket.upgrade_and_unwrap(),
            Err(_) => return false,
        };

        bucket.begin_read(token_name, timestamp).await.is_ok()
    }

    async fn wait_for_aggregate_count(repo: &AuditRepository, expected: usize) {
        for _ in 0..50 {
            let state = repo.state.read().await.unwrap();
            if state.aggregates.len() == expected {
                return;
            }
            drop(state);
            sleep(Duration::from_millis(20)).await;
        }

        let state = repo.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), expected);
    }

    #[tokio::test]
    async fn aggregates_events_with_same_key() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 2))
            .await
            .unwrap();

        wait_for_aggregate_count(&repo, 1).await;
        let state = repo.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), 1);
        let aggregate = state.aggregates.values().next().unwrap();
        assert_eq!(aggregate.call_count, 2);
        assert_eq!(aggregate.total_duration, 200);
        assert_eq!(aggregate.first_timestamp, 1);
        assert_eq!(aggregate.last_timestamp, 2);
    }

    #[tokio::test]
    async fn separates_events_with_different_status() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 403, 2))
            .await
            .unwrap();

        wait_for_aggregate_count(&repo, 2).await;
        let state = repo.state.read().await.unwrap();
        assert_eq!(state.aggregates.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flushes_single_event_after_delay() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();

        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.token_name, "token-1");
        assert_eq!(event.endpoint, "GET /api/v1/b/test");
        assert_eq!(event.status, 200);
        assert_eq!(event.call_count, 1);
        assert_eq!(event.duration, 100);
        assert_eq!(event.timestamp, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn second_matching_event_delays_flush_and_aggregates_record() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();
        sleep(Duration::from_secs(2)).await;
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 2))
            .await
            .unwrap();

        sleep(Duration::from_millis(3500)).await;
        assert!(!audit_record_exists(&repo, "token-1", 1).await);

        sleep(Duration::from_millis(2200)).await;
        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.call_count, 2);
        assert_eq!(event.duration, 200);
        assert_eq!(event.timestamp, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_timer_does_not_flush_after_version_change() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();
        sleep(Duration::from_secs(2)).await;
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 3))
            .await
            .unwrap();

        sleep(Duration::from_millis(3300)).await;
        assert!(!audit_record_exists(&repo, "token-1", 1).await);

        sleep(Duration::from_millis(2200)).await;
        let event = read_audit_event(&repo, "token-1", 1).await;
        assert_eq!(event.call_count, 2);
        assert_eq!(event.duration, 200);
        assert_eq!(event.timestamp, 1);
    }
}
