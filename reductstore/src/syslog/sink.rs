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
use crate::syslog::SystemEvent;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use std::sync::Arc;

#[async_trait]
pub(crate) trait LogSystemEvent {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError>;
}

pub(crate) type BoxedSystemLogger = Box<dyn LogSystemEvent + Send + Sync>;

/// Shared sink handle held by every event producer: the system logger behind an
/// async lock, plus the instance name stamped onto emitted events.
#[derive(Clone)]
pub(crate) struct SystemEventSink {
    pub(crate) system_logger: Arc<AsyncRwLock<BoxedSystemLogger>>,
    pub(crate) instance_name: String,
}
