// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{
    AuditAggregate, AuditAggregateKey, AuditEvent, AuditQuery, ManageAudit, AUDIT_BUCKET_NAME,
};
use crate::cfg::Cfg;
use crate::storage::bucket::Bucket;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::collections::HashMap;
use std::sync::Arc;

const AGGREGATION_WINDOW_US: u64 = 5_000_000;

// The local audit repository writes audit events into the internal $audit bucket.
pub(crate) struct AuditRepository {
    cfg: Cfg,
    storage: Arc<StorageEngine>,
    aggregates: HashMap<AuditAggregateKey, AuditAggregate>,
}

impl AuditRepository {
    pub async fn new(cfg: Cfg, storage: Arc<StorageEngine>) -> Self {
        Self {
            cfg,
            storage,
            aggregates: HashMap::new(),
        }
    }

    fn aggregate_event(&mut self, event: AuditEvent) {
        let key = AuditAggregateKey {
            token_name: event.token_name,
            endpoint: event.endpoint,
            status: event.status,
        };

        match self.aggregates.get_mut(&key) {
            Some(aggregate) => {
                aggregate.call_count += event.call_count;
                aggregate.total_duration += event.duration;
                aggregate.last_timestamp = event.timestamp;
                if event.timestamp < aggregate.first_timestamp {
                    aggregate.first_timestamp = event.timestamp;
                }
            }
            None => {
                self.aggregates.insert(
                    key,
                    AuditAggregate {
                        first_timestamp: event.timestamp,
                        last_timestamp: event.timestamp,
                        call_count: event.call_count,
                        total_duration: event.duration,
                    },
                );
            }
        }
    }

    async fn flush_expired(&mut self, now: u64) -> Result<(), ReductError> {
        let expired_keys: Vec<_> = self
            .aggregates
            .iter()
            .filter(|(_, aggregate)| {
                now.saturating_sub(aggregate.last_timestamp) > AGGREGATION_WINDOW_US
            })
            .map(|(key, _)| key.clone())
            .collect();

        self.flush_keys(expired_keys).await
    }

    async fn flush_keys(&mut self, keys: Vec<AuditAggregateKey>) -> Result<(), ReductError> {
        for key in keys {
            if let Some(aggregate) = self.aggregates.remove(&key) {
                self.write_aggregate(key, aggregate).await?;
            }
        }

        Ok(())
    }

    async fn write_aggregate(
        &mut self,
        key: AuditAggregateKey,
        aggregate: AuditAggregate,
    ) -> Result<(), ReductError> {
        let event = AuditEvent {
            timestamp: aggregate.first_timestamp,
            token_name: key.token_name,
            endpoint: key.endpoint,
            status: key.status,
            call_count: aggregate.call_count,
            duration: aggregate.total_duration,
        };

        self.write_event_to_bucket(event).await
    }

    async fn write_event_to_bucket(&mut self, event: AuditEvent) -> Result<(), ReductError> {
        let bucket = match self.storage.get_bucket(AUDIT_BUCKET_NAME).await {
            Ok(bucket) => bucket.upgrade()?,
            Err(_) => self
                .storage
                .create_bucket(AUDIT_BUCKET_NAME, Bucket::defaults())
                .await?
                .upgrade()?,
        };

        let labels = Labels::from([("endpoint".to_string(), event.endpoint.clone())]);
        let payload = serde_json::to_vec(&event).map_err(|err| {
            ReductError::internal_server_error(&format!("Failed to serialize audit event: {}", err))
        })?;
        let mut writer = bucket
            .begin_write(
                &event.token_name,
                event.timestamp,
                payload.len() as u64,
                "application/json".to_string(),
                labels,
            )
            .await?;
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
        Ok(())
    }
}

#[async_trait]
impl ManageAudit for AuditRepository {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError> {
        self.flush_expired(event.timestamp).await?;
        self.aggregate_event(event);
        Ok(())
    }

    async fn query_token_events(
        &mut self,
        _token_name: &str,
        _filter: AuditQuery,
    ) -> Result<Vec<AuditEvent>, ReductError> {
        // TODO: query audit records from the $audit bucket.
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::engine::StorageEngine;
    use bytes::Bytes;
    use reduct_base::io::ReadRecord;
    use tempfile::tempdir;

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

    #[tokio::test]
    async fn aggregates_events_with_same_key() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();
        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 2))
            .await
            .unwrap();

        assert_eq!(repo.aggregates.len(), 1);
        let aggregate = repo.aggregates.values().next().unwrap();
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

        assert_eq!(repo.aggregates.len(), 2);
    }

    #[tokio::test]
    async fn flushes_expired_aggregate_to_audit_bucket() {
        let mut repo = create_repo().await;

        repo.log_event(make_event("token-1", "GET /api/v1/b/test", 200, 1))
            .await
            .unwrap();
        repo.log_event(make_event(
            "token-1",
            "GET /api/v1/b/test",
            200,
            AGGREGATION_WINDOW_US + 2,
        ))
        .await
        .unwrap();
        repo.log_event(make_event(
            "token-2",
            "GET /api/v1/b/other",
            200,
            AGGREGATION_WINDOW_US + 3,
        ))
        .await
        .unwrap();

        assert_eq!(repo.aggregates.len(), 2);

        let bucket = repo
            .storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket.begin_read("token-1", 1).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: AuditEvent = serde_json::from_slice(&record).unwrap();

        assert_eq!(event.token_name, "token-1");
        assert_eq!(event.endpoint, "GET /api/v1/b/test");
        assert_eq!(event.status, 200);
        assert_eq!(event.call_count, 1);
        assert_eq!(event.duration, 100);
        assert_eq!(event.timestamp, 1);
        assert_eq!(record, Bytes::from(serde_json::to_vec(&event).unwrap()));
    }
}
