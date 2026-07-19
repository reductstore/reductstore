// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! The normalized system-event model shared by every `$system` producer.
//!
//! [`SystemEvent`] is the single record type all families emit; [`to_flat_json`]
//! is the one external serializer (the persisted `$system` record format).
//! [`SystemEventKind`] is the routing discriminant that lets one generic sink
//! place an event under `$system/<prefix>/<instance>/<entry>`.

use crate::syslog::{
    SYSTEM_AUDIT_ENTRY_PREFIX, SYSTEM_LIFECYCLE_ENTRY_PREFIX, SYSTEM_LOGS_ENTRY_PREFIX,
    SYSTEM_REPLICATION_ENTRY_PREFIX, SYSTEM_USAGE_ENTRY_PREFIX,
};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The family a system event belongs to. Determines the `$system/<prefix>/...`
/// entry path, so a single sink can route every family.
///
/// Carried on [`SystemEvent`] but **excluded from the serialized record**
/// (see [`SystemEvent::to_flat_json`]): adding it does not change the external
/// `$system` record format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SystemEventKind {
    #[default]
    Audit,
    Lifecycle,
    Replication,
    Usage,
    Log,
}

impl SystemEventKind {
    /// The `$system` entry-path prefix for this family. The single source of
    /// truth that replaces the per-logger prefix binding.
    pub(crate) fn entry_prefix(self) -> &'static str {
        match self {
            SystemEventKind::Audit => SYSTEM_AUDIT_ENTRY_PREFIX,
            SystemEventKind::Lifecycle => SYSTEM_LIFECYCLE_ENTRY_PREFIX,
            SystemEventKind::Replication => SYSTEM_REPLICATION_ENTRY_PREFIX,
            SystemEventKind::Usage => SYSTEM_USAGE_ENTRY_PREFIX,
            SystemEventKind::Log => SYSTEM_LOGS_ENTRY_PREFIX,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SystemEvent {
    /// Routing discriminant: which `$system/<prefix>` family this event belongs
    /// to. Skipped by serde so it never appears in the persisted record.
    #[serde(skip)]
    pub kind: SystemEventKind,
    /// Whether the persisted record may be picked up by a `$system`-source
    /// replication (issue #1457). Producers clear it for events that would feed
    /// back into the replication they describe: diagnostics of a `$system`
    /// replication and log messages emitted by the replication module. Skipped
    /// by serde so it never appears in the persisted record.
    #[serde(skip, default = "default_replicate")]
    pub replicate: bool,
    #[serde(default = "default_audit_type", rename = "type")]
    pub event_type: String,
    pub timestamp: u64,
    #[serde(default = "default_audit_instance")]
    pub instance: String,
    pub entry_name: String,
    pub status: u16,
    #[serde(default = "default_audit_message")]
    pub message: String,
    pub payload: Value,
}

impl SystemEvent {
    pub(crate) fn to_flat_json(&self) -> Result<Vec<u8>, ReductError> {
        let mut map = serde_json::Map::new();
        map.insert("timestamp".to_string(), serde_json::json!(self.timestamp));
        map.insert("instance".to_string(), serde_json::json!(self.instance));
        map.insert("status".to_string(), serde_json::json!(self.status));
        map.insert("message".to_string(), serde_json::json!(self.message));
        if let Value::Object(payload_map) = &self.payload {
            for (k, v) in payload_map {
                map.insert(k.clone(), v.clone());
            }
        }

        serde_json::to_vec(&map)
            .map_err(|err| internal_server_error!("Failed to serialize audit event: {}", err))
    }
}

fn default_replicate() -> bool {
    true
}

fn default_audit_type() -> String {
    "api_call".to_string()
}

fn default_audit_instance() -> String {
    "unknown".to_string()
}

fn default_audit_message() -> String {
    "".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_audit_type_is_api_call() {
        assert_eq!(default_audit_type(), "api_call");
    }

    #[test]
    fn default_audit_instance_is_unknown() {
        assert_eq!(default_audit_instance(), "unknown");
    }

    #[test]
    fn default_audit_message_is_empty() {
        assert_eq!(default_audit_message(), "");
    }

    #[test]
    fn entry_prefix_maps_each_kind_to_its_path_segment() {
        assert_eq!(SystemEventKind::Audit.entry_prefix(), "audit");
        assert_eq!(SystemEventKind::Lifecycle.entry_prefix(), "lifecycle");
        assert_eq!(SystemEventKind::Replication.entry_prefix(), "replications");
        assert_eq!(SystemEventKind::Usage.entry_prefix(), "usage");
        assert_eq!(SystemEventKind::Log.entry_prefix(), "logs");
    }

    #[test]
    fn kind_is_excluded_from_flat_json() {
        let event = SystemEvent {
            kind: SystemEventKind::Lifecycle,
            replicate: true,
            event_type: "lifecycle_run".to_string(),
            timestamp: 1,
            instance: "node".to_string(),
            entry_name: "policy".to_string(),
            status: 200,
            message: "".to_string(),
            payload: serde_json::json!({"policy_name": "p"}),
        };
        let value: Value = serde_json::from_slice(&event.to_flat_json().unwrap()).unwrap();
        let object = value.as_object().unwrap();
        assert!(!object.contains_key("kind"));
        assert!(!object.contains_key("type"));
        assert!(!object.contains_key("entry_name"));
    }
}
