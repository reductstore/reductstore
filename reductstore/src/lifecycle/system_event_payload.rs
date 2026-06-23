// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct LifecycleSystemEventPayload {
    pub policy_name: String,
    pub action_type: String,
    pub bucket: String,
    pub duration: f64,
    pub processed_records: u64,
    pub processed_blocks: u64,
    pub last_processed_ts: Option<u64>,
    pub caught_up: bool,
    pub error_code: Option<u16>,
    #[serde(default)]
    pub error_message: Option<String>,
}

impl LifecycleSystemEventPayload {
    pub(crate) fn success(
        policy_name: &str,
        action_type: &str,
        bucket: &str,
        duration: f64,
        processed_records: u64,
        processed_blocks: Option<u64>,
        last_processed_ts: Option<u64>,
        caught_up: bool,
    ) -> Self {
        Self {
            policy_name: policy_name.to_string(),
            action_type: action_type.to_string(),
            bucket: bucket.to_string(),
            duration,
            processed_records,
            processed_blocks: processed_blocks.unwrap_or(0),
            last_processed_ts,
            caught_up,
            error_code: None,
            error_message: None,
        }
    }

    pub(crate) fn error(
        policy_name: &str,
        action_type: &str,
        bucket: &str,
        duration: f64,
        error_code: u16,
        error_message: &str,
    ) -> Self {
        Self {
            policy_name: policy_name.to_string(),
            action_type: action_type.to_string(),
            bucket: bucket.to_string(),
            duration,
            processed_records: 0,
            processed_blocks: 0,
            last_processed_ts: None,
            caught_up: false,
            error_code: Some(error_code),
            error_message: Some(error_message.to_string()),
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        let mut payload = json!({
            "policy_name": self.policy_name,
            "action_type": self.action_type,
            "bucket": self.bucket,
            "duration": self.duration,
            "processed_records": self.processed_records,
            "processed_blocks": self.processed_blocks,
            "caught_up": self.caught_up,
        });

        if let Some(error_code) = self.error_code {
            payload["error_code"] = json!(error_code);
        }

        if let Some(last_processed_ts) = self.last_processed_ts {
            payload["last_processed_ts"] = json!(last_processed_ts);
        }

        if let Some(error_message) = &self.error_message {
            payload["error_message"] = json!(error_message);
        }

        payload
    }
}
