// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Aggregated replication diagnostics carried by a replication system event.
///
/// The schema is identical for success and failure events so downstream
/// parsing is uniform: a success event has `written_records = N` and
/// `failed_records = 0`, a failure event has `failed_records = X` and
/// `written_records = 0`, but both carry every field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct ReplicationSystemEventPayload {
    pub status: u16,
    pub pending_records: u64,
    pub written_records: u64,
    pub failed_records: u64,
    pub replicated_data_size: u64,
    pub compressed_data_size: u64,
    pub duration: f64,
}

impl ReplicationSystemEventPayload {
    pub(crate) fn to_value(&self) -> Value {
        json!({
            "status": self.status,
            "pending_records": self.pending_records,
            "written_records": self.written_records,
            "failed_records": self.failed_records,
            "replicated_data_size": self.replicated_data_size,
            "compressed_data_size": self.compressed_data_size,
            "duration": self.duration,
        })
    }
}
