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
