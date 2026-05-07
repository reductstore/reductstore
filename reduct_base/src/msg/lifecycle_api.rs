// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Lifecycle policy action type.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LifecycleType {
    /// Delete records matched by lifecycle settings.
    #[default]
    Delete,
}

/// Lifecycle policy settings.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct LifecycleSettings {
    /// Lifecycle policy action type.
    #[serde(default, rename = "type")]
    pub lifecycle_type: LifecycleType,
    /// Bucket to apply the lifecycle policy to.
    pub bucket: String,
    /// Entries to clean. If empty, all removable entries are matched. Prefix wildcards are supported.
    #[serde(default)]
    pub entries: Vec<String>,
    /// Maximum record age, e.g. "30d", "24h", or "3600s".
    pub max_age: String,
    /// Interval in seconds between lifecycle runs.
    #[serde(default = "default_lifecycle_interval_secs")]
    pub interval: u64,
    /// When condition.
    #[serde(default)]
    pub when: Option<Value>,
}

fn default_lifecycle_interval_secs() -> u64 {
    3600
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
