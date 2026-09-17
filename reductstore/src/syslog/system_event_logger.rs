// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! The single generic sink facade: builds the one logger that accepts
//! normalized [`SystemEvent`](crate::syslog::SystemEvent)s and persists them
//! locally (primary / standalone) or forwards them to the primary (read-only
//! replica). It owns no family-specific aggregation — path resolution comes
//! from the event's kind ([`path`](crate::syslog::path)).

use crate::cfg::Cfg;
use crate::storage::engine::StorageEngine;
use crate::syslog::forward_writer::ForwardSystemLogger;
use crate::syslog::local_writer::LocalSystemLogger;
use crate::syslog::BoxedSystemLogger;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::BucketSettings;
use std::sync::Arc;

/// Builds the generic system-event logger. The entry-path prefix is no longer
/// bound here — it travels with each event via its
/// [`SystemEventKind`](crate::syslog::SystemEventKind).
pub(crate) struct SystemEventLoggerBuilder {
    bucket_name: &'static str,
    bucket_settings: BucketSettings,
}

impl SystemEventLoggerBuilder {
    pub(crate) fn new(bucket_name: &'static str, bucket_settings: BucketSettings) -> Self {
        Self {
            bucket_name,
            bucket_settings,
        }
    }

    pub(crate) fn build(
        self,
        cfg: &Cfg,
        storage: Arc<StorageEngine>,
    ) -> Result<BoxedSystemLogger, ReductError> {
        if cfg.role == crate::cfg::InstanceRole::Replica {
            Ok(Box::new(ForwardSystemLogger::new(self.bucket_name, cfg)?))
        } else {
            Ok(Box::new(LocalSystemLogger::new(
                self.bucket_name,
                self.bucket_settings,
                storage,
            )))
        }
    }
}
