// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0
use crate::msg::diagnostics::Diagnostics;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Replication mode
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationMode {
    /// Replication is active and sends records
    Enabled,
    /// Replication stores transactions but doesn't send them
    Paused,
    /// Replication ignores new transactions
    Disabled,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Enabled
    }
}

/// Compression of batch payloads during transfer
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationCompression {
    /// No compression
    #[default]
    None,
    /// Zstandard compression
    Zstd,
    /// Gzip compression
    Gzip,
}

/// Replication settings
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct ReplicationSettings {
    /// Source bucket
    pub src_bucket: String,
    /// Destination bucket
    pub dst_bucket: String,
    /// Destination host URL (e.g. https://reductstore.com).
    /// If empty, the destination bucket is in the same instance
    /// and records are copied to it directly, without any URL or token.
    #[serde(default)]
    pub dst_host: String,
    /// Destination access token. It is not used if the destination bucket is in the same instance.
    pub dst_token: Option<String>,
    /// Entries to replicate. If empty, all entries are replicated. Supports exact names,
    /// glob-like `*` and `**` wildcards, and `!` exclusion patterns.
    #[serde(default)]
    pub entries: Vec<String>,
    /// Prefix to add to destination entry names
    #[serde(default)]
    pub dst_prefix: String,
    /// When condition
    #[serde(default)]
    pub when: Option<Value>,
    /// Mode
    #[serde(default)]
    pub mode: ReplicationMode,
    /// Compression of batch payloads during transfer
    #[serde(default)]
    pub compression: ReplicationCompression,
}

/// Replication info
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ReplicationInfo {
    /// Replication name
    pub name: String,
    /// Replication mode
    #[serde(default)] // for backward compatibility with older versions of reduct-rs
    pub mode: ReplicationMode,
    /// Remote instance is available and replication is active
    pub is_active: bool,
    /// Replication is provisioned
    pub is_provisioned: bool,
    /// Number of records pending replication
    pub pending_records: u64,
}

/// Payload for updating replication mode
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationModePayload {
    pub mode: ReplicationMode,
}

/// Replication list
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct ReplicationList {
    /// Replication list
    pub replications: Vec<ReplicationInfo>,
}

/// Replication settings
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FullReplicationInfo {
    /// Info
    pub info: ReplicationInfo,
    /// Settings
    pub settings: ReplicationSettings,
    /// Diagnostics
    pub diagnostics: Diagnostics,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::omitted(r#"{"src_bucket":"bucket-1","dst_bucket":"bucket-2"}"#)]
    #[case::null_token(r#"{"src_bucket":"bucket-1","dst_bucket":"bucket-2","dst_token":null}"#)]
    #[case::empty_host(r#"{"src_bucket":"bucket-1","dst_bucket":"bucket-2","dst_host":""}"#)]
    fn settings_without_host_and_token_are_local(#[case] json: &str) {
        let settings: ReplicationSettings = serde_json::from_str(json).unwrap();

        assert_eq!(settings.src_bucket, "bucket-1");
        assert_eq!(settings.dst_bucket, "bucket-2");
        assert_eq!(settings.dst_host, "");
        assert_eq!(settings.dst_token, None);
    }

    #[test]
    fn settings_keep_host_and_token() {
        let settings: ReplicationSettings = serde_json::from_str(
            r#"{
                "src_bucket": "bucket-1",
                "dst_bucket": "bucket-2",
                "dst_host": "http://localhost:8383",
                "dst_token": "token"
            }"#,
        )
        .unwrap();

        assert_eq!(settings.dst_host, "http://localhost:8383");
        assert_eq!(settings.dst_token, Some("token".to_string()));
    }

    #[test]
    fn local_settings_roundtrip() {
        let settings = ReplicationSettings {
            src_bucket: "bucket-1".to_string(),
            dst_bucket: "bucket-2".to_string(),
            ..Default::default()
        };

        let json = serde_json::to_string(&settings).unwrap();
        assert_eq!(
            serde_json::from_str::<ReplicationSettings>(&json).unwrap(),
            settings
        );
    }
}
