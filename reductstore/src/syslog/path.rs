// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! `$system` entry-path and record-label resolution, shared by both writers.
//!
//! Previously the `<prefix>/<instance>/<entry>` path and the `unknown`
//! empty-instance fallback were duplicated in the local and forward writers.
//! This module is the single home for both, deriving the prefix from
//! [`SystemEvent::kind`](crate::syslog::SystemEvent).

use crate::syslog::SystemEvent;
use reduct_base::Labels;

/// Resolve the `$system` entry path `<kind-prefix>/<instance>/<entry>`.
///
/// An empty instance falls back to `unknown`, matching both writers' prior
/// behavior.
pub(crate) fn entry_path(event: &SystemEvent) -> String {
    let instance = if event.instance.is_empty() {
        "unknown"
    } else {
        event.instance.as_str()
    };
    format!(
        "{}/{}/{}",
        event.kind.entry_prefix(),
        instance,
        event.entry_name
    )
}

/// Build the record labels for an event, matching the local writer's prior
/// behavior: always a `status` label, plus a `level` label when the payload
/// carries one (log events expose severity as a queryable label).
pub(crate) fn record_labels(event: &SystemEvent) -> Labels {
    let mut labels = Labels::from([("status".to_string(), event.status.to_string())]);
    if let Some(level) = event.payload.get("level").and_then(|value| value.as_str()) {
        labels.insert("level".to_string(), level.to_string());
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::syslog::{SystemEvent, SystemEventKind};

    fn event(kind: SystemEventKind, instance: &str, payload: serde_json::Value) -> SystemEvent {
        SystemEvent {
            kind,
            event_type: "t".to_string(),
            timestamp: 1,
            instance: instance.to_string(),
            entry_name: "entry".to_string(),
            status: 200,
            message: "".to_string(),
            payload,
        }
    }

    #[test]
    fn entry_path_uses_kind_prefix() {
        let e = event(
            SystemEventKind::Replication,
            "node-1",
            serde_json::json!({}),
        );
        assert_eq!(entry_path(&e), "replications/node-1/entry");
    }

    #[test]
    fn entry_path_empty_instance_falls_back_to_unknown() {
        let e = event(SystemEventKind::Audit, "", serde_json::json!({}));
        assert_eq!(entry_path(&e), "audit/unknown/entry");
    }

    #[test]
    fn record_labels_has_status_and_optional_level() {
        let without = event(SystemEventKind::Usage, "n", serde_json::json!({}));
        let labels = record_labels(&without);
        assert_eq!(labels.get("status").map(String::as_str), Some("200"));
        assert!(!labels.contains_key("level"));

        let with = event(
            SystemEventKind::Log,
            "n",
            serde_json::json!({"level": "WARN"}),
        );
        let labels = record_labels(&with);
        assert_eq!(labels.get("level").map(String::as_str), Some("WARN"));
    }
}
