// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

pub(crate) mod aggregator;

use crate::audit::AuditEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct ApiAuditPayload {
    pub token_name: String,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub client_ip: Option<String>,
    pub call_count: u64,
    pub duration: f64,
}

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

impl ApiAuditPayload {
    pub(crate) fn from_event(event: &AuditEvent) -> Option<Self> {
        if let Some(payload) = &event.payload {
            serde_json::from_value::<ApiAuditPayload>(payload.clone()).ok()
        } else {
            Some(Self {
                token_name: event.token_name.clone(),
                method: event.method.clone(),
                path: event.path.clone(),
                client_ip: event.client_ip.clone(),
                call_count: event.call_count,
                duration: event.duration,
            })
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        json!({
            "token_name": self.token_name,
            "method": self.method,
            "path": self.path,
            "client_ip": self.client_ip,
            "call_count": self.call_count,
            "duration": self.duration,
        })
    }
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
