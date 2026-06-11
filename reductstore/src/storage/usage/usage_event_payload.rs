// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Usage statistics carried by a usage system event.
///
/// The first five fields describe the flush interval: `duration` is the
/// measured elapsed time between flushes in seconds, the counters are the
/// traffic tallied during that interval. The last four fields are a
/// point-in-time snapshot of the storage taken at flush time.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct UsageSystemEventPayload {
    pub duration: f64,
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub records_written: u64,
    pub records_read: u64,
    pub storage_bytes: u64,
    pub bucket_count: u64,
    pub entry_count: u64,
    pub block_count: u64,
}

impl UsageSystemEventPayload {
    pub(crate) fn to_value(&self) -> Value {
        json!({
            "duration": self.duration,
            "write_bytes": self.write_bytes,
            "read_bytes": self.read_bytes,
            "records_written": self.records_written,
            "records_read": self.records_read,
            "storage_bytes": self.storage_bytes,
            "bucket_count": self.bucket_count,
            "entry_count": self.entry_count,
            "block_count": self.block_count,
        })
    }
}
