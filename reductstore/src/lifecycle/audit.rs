// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct LifecycleAuditPayload {
    pub policy_name: String,
    pub action_type: String,
    pub bucket: String,
    pub duration: f64,
    #[serde(default)]
    pub processed_records: Option<u64>,
    #[serde(default)]
    pub error_code: Option<u16>,
    #[serde(default)]
    pub error_message: Option<String>,
}

impl LifecycleAuditPayload {
    pub(crate) fn success(
        policy_name: &str,
        action_type: &str,
        bucket: &str,
        duration: f64,
        processed_records: u64,
    ) -> Self {
        Self {
            policy_name: policy_name.to_string(),
            action_type: action_type.to_string(),
            bucket: bucket.to_string(),
            duration,
            processed_records: Some(processed_records),
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
            processed_records: None,
            error_code: Some(error_code),
            error_message: Some(error_message.to_string()),
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        json!({
            "policy_name": self.policy_name,
            "action_type": self.action_type,
            "bucket": self.bucket,
            "duration": self.duration,
            "processed_records": self.processed_records,
            "error_code": self.error_code,
            "error_message": self.error_message,
        })
    }
}
