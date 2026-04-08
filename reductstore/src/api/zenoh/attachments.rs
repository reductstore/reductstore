// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use reduct_base::Labels;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) fn serialize_labels(labels: &Labels) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(labels)
}

pub(crate) fn deserialize_labels(payload: &[u8]) -> Result<Labels, serde_json::Error> {
    serde_json::from_slice(payload)
}

/// Query attachments that can be passed with Zenoh get() requests.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub(crate) struct QueryAttachments {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<Value>,
}

pub(crate) fn deserialize_query_attachments(
    payload: &[u8],
) -> Result<QueryAttachments, serde_json::Error> {
    serde_json::from_slice(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_labels() {
        let mut labels = Labels::new();
        labels.insert("sensor".into(), "imu".into());
        labels.insert("unit".into(), "m/s^2".into());

        let encoded = serialize_labels(&labels).expect("encode labels");
        let decoded = deserialize_labels(&encoded).expect("decode labels");

        assert_eq!(decoded, labels);
    }

    #[test]
    fn deserialize_invalid_payload() {
        let result = deserialize_labels(b"not-json");
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_query_attachments_with_when() {
        let payload = br#"{"when": {"&label": "value"}}"#;
        let attachments = deserialize_query_attachments(payload).expect("decode attachments");

        assert_eq!(attachments.when, Some(json!({"&label": "value"})));
    }

    #[test]
    fn deserialize_empty_query_attachments() {
        let payload = b"{}";
        let attachments = deserialize_query_attachments(payload).expect("decode attachments");

        assert_eq!(attachments.when, None);
    }
}
