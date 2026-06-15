// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Lifecycle policy action type.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LifecycleType {
    /// Delete records matched by lifecycle settings.
    #[default]
    Delete,
    /// Compress blocks matched by lifecycle settings.
    Compress,
}

/// Lifecycle mode.
#[repr(u8)]
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LifecycleMode {
    /// Lifecycle is active and executes actions.
    Enabled,
    /// Lifecycle is inactive and does not execute actions.
    Disabled,
    /// Lifecycle runs in preview mode and does not remove records.
    #[serde(rename = "dry_run")]
    DryRun,
}

impl Default for LifecycleMode {
    fn default() -> Self {
        Self::Enabled
    }
}

/// Lifecycle policy settings.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LifecycleSettings {
    /// Lifecycle policy action type.
    #[serde(rename = "type")]
    pub lifecycle_type: LifecycleType,
    /// Bucket to apply the lifecycle policy to.
    pub bucket: String,
    /// Entries to clean. If empty, all removable entries are matched. Prefix wildcards are supported.
    /// System metadata entries (e.g. `entry/$meta`) are always excluded.
    #[serde(default)]
    pub entries: Vec<String>,
    /// Records older than this duration, e.g. "30d", "24h", or "3600s".
    pub older_than: String,
    /// Interval between lifecycle runs, e.g. "30m", "1h", or "3600s".
    #[serde(default = "default_lifecycle_interval")]
    pub interval: String,
    /// When condition.
    #[serde(default)]
    pub when: Option<Value>,
    /// Lifecycle mode.
    #[serde(default)]
    pub mode: LifecycleMode,
}

impl Default for LifecycleSettings {
    fn default() -> Self {
        Self {
            lifecycle_type: LifecycleType::default(),
            bucket: String::default(),
            entries: Vec::default(),
            older_than: String::default(),
            interval: default_lifecycle_interval(),
            when: Option::default(),
            mode: LifecycleMode::default(),
        }
    }
}

fn default_lifecycle_interval() -> String {
    "3600s".to_string()
}

/// Lifecycle policy information.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LifecycleInfo {
    /// Lifecycle policy name.
    pub name: String,
    /// Lifecycle policy is provisioned.
    pub is_provisioned: bool,
    /// Lifecycle worker is running.
    pub is_running: bool,
    /// Lifecycle policy action type.
    #[serde(default, rename = "type")]
    pub lifecycle_type: LifecycleType,
    /// Lifecycle mode.
    #[serde(default)]
    pub mode: LifecycleMode,
    /// Last lifecycle run timestamp.
    #[serde(default)]
    pub last_run: Option<DateTime<Utc>>,
}

/// Payload for updating lifecycle mode.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleModePayload {
    pub mode: LifecycleMode,
}

/// Lifecycle policy list.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct LifecycleList {
    /// Lifecycle policies.
    pub lifecycles: Vec<LifecycleInfo>,
}

/// Full lifecycle policy information.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FullLifecycleInfo {
    /// Info.
    pub info: LifecycleInfo,
    /// Settings.
    pub settings: LifecycleSettings,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_mode_default_is_enabled() {
        assert_eq!(LifecycleMode::default(), LifecycleMode::Enabled);
    }

    #[test]
    fn lifecycle_mode_serde_roundtrip() {
        assert_eq!(
            serde_json::to_string(&LifecycleMode::Enabled).unwrap(),
            "\"enabled\""
        );
        assert_eq!(
            serde_json::to_string(&LifecycleMode::Disabled).unwrap(),
            "\"disabled\""
        );
        assert_eq!(
            serde_json::to_string(&LifecycleMode::DryRun).unwrap(),
            "\"dry_run\""
        );

        assert_eq!(
            serde_json::from_str::<LifecycleMode>("\"enabled\"").unwrap(),
            LifecycleMode::Enabled
        );
        assert_eq!(
            serde_json::from_str::<LifecycleMode>("\"disabled\"").unwrap(),
            LifecycleMode::Disabled
        );
        assert_eq!(
            serde_json::from_str::<LifecycleMode>("\"dry_run\"").unwrap(),
            LifecycleMode::DryRun
        );
    }

    #[test]
    fn lifecycle_settings_mode_defaults_on_missing() {
        let settings: LifecycleSettings = serde_json::from_str(
            r#"{
                "type": "delete",
                "bucket": "bucket-1",
                "entries": ["entry-1"],
                "older_than": "1d",
                "interval": "1h"
            }"#,
        )
        .unwrap();

        assert_eq!(settings.mode, LifecycleMode::Enabled);
    }

    #[test]
    fn lifecycle_settings_type_is_required() {
        let err = serde_json::from_str::<LifecycleSettings>(
            r#"{
                "bucket": "bucket-1",
                "older_than": "1d"
            }"#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("missing field `type`"));
    }

    #[test]
    fn lifecycle_settings_default_interval_is_3600s() {
        let settings = LifecycleSettings::default();
        assert_eq!(settings.interval, "3600s");
    }

    #[test]
    fn lifecycle_info_mode_defaults_on_missing() {
        let info: LifecycleInfo = serde_json::from_str(
            r#"{
                "name": "test",
                "is_provisioned": false,
                "is_running": true
            }"#,
        )
        .unwrap();

        assert_eq!(info.lifecycle_type, LifecycleType::Delete);
        assert_eq!(info.mode, LifecycleMode::Enabled);
        assert_eq!(info.last_run, None);
    }

    #[test]
    fn lifecycle_mode_payload_serde_roundtrip() {
        let payload = LifecycleModePayload {
            mode: LifecycleMode::Disabled,
        };

        let serialized = serde_json::to_string(&payload).unwrap();
        assert_eq!(serialized, r#"{"mode":"disabled"}"#);

        let deserialized: LifecycleModePayload = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, payload);
    }
}
