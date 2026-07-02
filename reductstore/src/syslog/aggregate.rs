// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Event producers: the aggregators and periodic flush workers that decide
//! *when* and *how* to emit each `$system` event family, all writing through
//! the shared sink.

pub(crate) mod audit;
pub(crate) mod replication;
pub(crate) mod usage;

/// Sliding idle-window before an open aggregate is flushed, shared by the audit
/// and replication aggregators. One second under test so the windows elapse
/// quickly.
#[cfg(not(test))]
pub(crate) const AGGREGATION_WINDOW_SECS: u64 = 5;
#[cfg(test)]
pub(crate) const AGGREGATION_WINDOW_SECS: u64 = 1;

/// Hard time-cap after which an aggregate is force-flushed even if it is still
/// receiving events.
pub(crate) const FORCE_FLUSH_TIMEOUT_SECS: u64 = 60;
