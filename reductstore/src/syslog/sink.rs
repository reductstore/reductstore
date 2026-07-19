// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! The write interface every `$system` producer depends on.
//!
//! [`LogSystemEvent`] is the trait implemented by the local and forward writers
//! (and by the disabled/aggregating wrappers). [`SystemEventSink`] is the
//! shared handle producers hold: a logger behind an async lock plus the
//! instance name. It lives here — not in any one family's module — because all
//! five families share it.

use crate::core::sync::AsyncRwLock;
use crate::replication::TransactionNotification;
use crate::syslog::SystemEvent;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Late-bound callback that routes a [`TransactionNotification`] into
/// `ManageReplications::notify`.
pub(crate) type ReplicationNotifier = Arc<
    dyn Fn(TransactionNotification) -> Pin<Box<dyn Future<Output = Result<(), ReductError>> + Send>>
        + Send
        + Sync,
>;

#[async_trait]
pub(crate) trait LogSystemEvent {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError>;

    /// Register (`Some`) or clear (`None`) the replication notifier.
    /// No-op by default; only the local writer stores it.
    async fn set_replication_notifier(&mut self, _notifier: Option<ReplicationNotifier>) {}
}

pub(crate) type BoxedSystemLogger = Box<dyn LogSystemEvent + Send + Sync>;

/// Shared sink handle held by every event producer: the system logger behind an
/// async lock, plus the instance name stamped onto emitted events.
#[derive(Clone)]
pub(crate) struct SystemEventSink {
    pub(crate) system_logger: Arc<AsyncRwLock<BoxedSystemLogger>>,
    pub(crate) instance_name: String,
}
