use reduct_base::Labels;

/// Encodes ReductStore labels as a JSON attachment payload compatible with Zenoh samples.
pub(crate) fn serialize_labels(labels: &Labels) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(labels)
}

/// Restores labels from a JSON attachment payload.
pub(crate) fn deserialize_labels(payload: &[u8]) -> Result<Labels, serde_json::Error> {
    serde_json::from_slice(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
