// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::AuditEvent;
use crate::core::sync::AsyncRwLock;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

pub(super) const AGGREGATION_WINDOW_SECS: u64 = 5;
pub(super) const AUDIT_CHANNEL_SIZE: usize = 1024;

pub(super) type FlushFuture = Pin<Box<dyn Future<Output = Result<(), ReductError>> + Send>>;
pub(super) type FlushHandler = Arc<dyn Fn(AuditEvent) -> FlushFuture + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct AuditAggregateKey {
    pub token_name: String,
    pub endpoint: String,
    pub status: u16,
}

#[derive(Debug, Clone)]
pub(super) struct AuditAggregate {
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub call_count: u64,
    pub total_duration: u64,
    pub flush_at: Instant,
}

#[derive(Default)]
pub(super) struct AuditState {
    pub aggregates: HashMap<AuditAggregateKey, AuditAggregate>,
}

pub(super) fn make_event(key: AuditAggregateKey, aggregate: AuditAggregate) -> AuditEvent {
    AuditEvent {
        timestamp: aggregate.first_timestamp,
        token_name: key.token_name,
        endpoint: key.endpoint,
        status: key.status,
        call_count: aggregate.call_count,
        duration: aggregate.total_duration,
    }
}

pub(super) struct AuditAggregator {
    tx: mpsc::Sender<AuditEvent>,
    #[cfg(test)]
    pub state: Arc<AsyncRwLock<AuditState>>,
}

impl AuditAggregator {
    pub(super) fn new(handler: FlushHandler) -> Self {
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(AUDIT_CHANNEL_SIZE);

        tokio::spawn(Self::run_worker(rx, Arc::clone(&state), handler));

        Self {
            tx,
            #[cfg(test)]
            state,
        }
    }

    pub(super) async fn log_event(&self, event: AuditEvent) -> Result<(), ReductError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| internal_server_error!("Audit worker is not available"))
    }

    async fn run_worker(
        mut rx: mpsc::Receiver<AuditEvent>,
        state: Arc<AsyncRwLock<AuditState>>,
        handler: FlushHandler,
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
                                let _ = Self::flush_all(&state, &handler).await;
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        let _ = Self::flush_expired(&state, &handler).await;
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
        state
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

        Ok(())
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
        handler: &FlushHandler,
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
                        .map(|aggregate| make_event(key, aggregate))
                })
                .collect::<Vec<_>>()
        };

        for event in events {
            handler(event).await?;
        }

        Ok(())
    }

    async fn flush_all(
        state: &Arc<AsyncRwLock<AuditState>>,
        handler: &FlushHandler,
    ) -> Result<(), ReductError> {
        let events = {
            let mut state = state.write().await?;
            state
                .aggregates
                .drain()
                .map(|(key, aggregate)| make_event(key, aggregate))
                .collect::<Vec<_>>()
        };

        for event in events {
            handler(event).await?;
        }

        Ok(())
    }
}
