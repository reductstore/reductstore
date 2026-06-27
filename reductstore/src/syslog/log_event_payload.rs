// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Payload carried by a `log_message` system event. Mirrors the other
/// `*_event_payload` structs: a flat set of fields that `to_value` renders into
/// the JSON object flattened by [`SystemEvent::to_flat_json`](crate::syslog::SystemEvent::to_flat_json).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct LogSystemEventPayload {
    pub level: String,
    pub target: String,
    pub file: Option<String>,
    pub line: Option<u32>,
}

impl LogSystemEventPayload {
    pub(crate) fn to_value(&self) -> Value {
        json!({
            "level": self.level,
            "target": self.target,
            "file": self.file,
            "line": self.line,
        })
    }
}
