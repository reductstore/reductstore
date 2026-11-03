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
/// Dynamic segments are reduced to the storage-safe character set without
/// changing the event itself, so the JSON payload preserves producer values.
pub(crate) fn entry_path(event: &SystemEvent) -> String {
    format!(
        "{}/{}/{}",
        event.kind.entry_prefix(),
        sanitize_path_segment(&event.instance),
        sanitize_path_segment(&event.entry_name)
    )
}

/// Convert a dynamically produced path segment to an entry-name-safe segment.
pub(crate) fn sanitize_path_segment(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
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
            replicate: true,
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

    #[rstest::rstest]
    #[case("plant.example.com", "SP.9322", "plant_example_com", "SP_9322")]
    #[case("plant:west", "SP 9322", "plant_west", "SP_9322")]
    #[case("plant/node", "$meta", "plant_node", "_meta")]
    fn entry_path_sanitizes_dynamic_segments(
        #[case] instance: &str,
        #[case] entry_name: &str,
        #[case] expected_instance: &str,
        #[case] expected_entry_name: &str,
    ) {
        let mut e = event(SystemEventKind::Lifecycle, instance, serde_json::json!({}));
        e.entry_name = entry_name.to_string();

        assert_eq!(
            entry_path(&e),
            format!("lifecycle/{expected_instance}/{expected_entry_name}")
        );
        assert_eq!(e.instance, instance);
        assert_eq!(e.entry_name, entry_name);
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
