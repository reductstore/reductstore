// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::deployment_id::{NodeId, StoreId};
use crate::replication::{
    REPLICATION_LICENSE_HASH_HEADER, REPLICATION_NODE_ID_HEADER, REPLICATION_STORE_ID_HEADER,
};
use axum::http::HeaderMap;
use parking_lot::Mutex;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::server_api::License;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEVICE_IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const INVALID_LICENSE_MESSAGE: &str = "Invalid replication license identity";
const DEVICE_LIMIT_MESSAGE: &str = "Replication device limit reached";
const ACTIVE_NODE_MESSAGE: &str = "Replication node is already active for this device";

#[derive(Debug, PartialEq)]
struct ReplicationIdentity {
    node_id: String,
    store_id: String,
    license_hash: String,
}

struct ActiveNode {
    node_id: String,
    last_seen: Instant,
}

pub(crate) struct LicenseDeviceGuard {
    license: Option<License>,
    receiver_store_id: Uuid,
    receiver_node_id: String,
    devices: Mutex<HashMap<Uuid, ActiveNode>>,
}

impl LicenseDeviceGuard {
    pub(crate) fn new(
        license: Option<License>,
        receiver_store_id: StoreId,
        receiver_node_id: NodeId,
    ) -> Self {
        Self::with_receiver_identity(
            license,
            receiver_store_id.as_uuid(),
            receiver_node_id.to_string(),
        )
    }

    fn with_receiver_identity(
        license: Option<License>,
        receiver_store_id: Uuid,
        receiver_node_id: String,
    ) -> Self {
        Self {
            license,
            receiver_store_id,
            receiver_node_id,
            devices: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn validate_headers(&self, headers: &HeaderMap) -> Result<(), ReductError> {
        if self.license.is_some() {
            parse_identity(headers)?;
        }
        Ok(())
    }

    pub(crate) fn check(&self, headers: &HeaderMap) -> Result<(), ReductError> {
        let Some(license) = &self.license else {
            return Ok(());
        };

        let Some(identity) = parse_identity(headers)? else {
            return Ok(());
        };

        if identity.license_hash == "null" || identity.license_hash != license.fingerprint {
            return Err(invalid_license_error());
        }

        let store_id = Uuid::parse_str(&identity.store_id).map_err(|_| invalid_license_error())?;
        self.check_at(
            store_id,
            identity.node_id,
            Instant::now(),
            license.device_number,
        )
    }

    fn check_at(
        &self,
        store_id: Uuid,
        node_id: String,
        now: Instant,
        device_number: u32,
    ) -> Result<(), ReductError> {
        if store_id == self.receiver_store_id {
            return if node_id == self.receiver_node_id {
                Ok(())
            } else {
                Err(active_node_error())
            };
        }

        if device_number == 0 {
            return Ok(());
        }

        let mut devices = self.devices.lock();
        devices.retain(|_, active_node| {
            now.duration_since(active_node.last_seen) <= DEVICE_IDLE_TIMEOUT
        });

        if let Some(active_node) = devices.get_mut(&store_id) {
            if active_node.node_id == node_id {
                active_node.last_seen = now;
                return Ok(());
            }

            return Err(active_node_error());
        }

        if devices.len() >= device_number as usize {
            return Err(device_limit_error());
        }

        devices.insert(
            store_id,
            ActiveNode {
                node_id,
                last_seen: now,
            },
        );
        Ok(())
    }
}

fn parse_identity(headers: &HeaderMap) -> Result<Option<ReplicationIdentity>, ReductError> {
    let values = [
        headers.get(REPLICATION_NODE_ID_HEADER),
        headers.get(REPLICATION_STORE_ID_HEADER),
        headers.get(REPLICATION_LICENSE_HASH_HEADER),
    ];
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }

    let values: Vec<&str> = values
        .into_iter()
        .map(|value| {
            value
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
        })
        .collect::<Option<_>>()
        .ok_or_else(invalid_license_error)?;
    Ok(Some(ReplicationIdentity {
        node_id: values[0].to_string(),
        store_id: values[1].to_string(),
        license_hash: values[2].to_string(),
    }))
}

fn invalid_license_error() -> ReductError {
    ReductError::new(ErrorCode::TooManyRequests, INVALID_LICENSE_MESSAGE)
}

fn active_node_error() -> ReductError {
    ReductError::new(ErrorCode::TooManyRequests, ACTIVE_NODE_MESSAGE)
}

fn device_limit_error() -> ReductError {
    ReductError::new(ErrorCode::TooManyRequests, DEVICE_LIMIT_MESSAGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use chrono::Utc;
    use rstest::{fixture, rstest};

    fn license(device_number: u32) -> License {
        License {
            licensee: String::new(),
            invoice: String::new(),
            expiry_date: Utc::now(),
            plan: String::new(),
            device_number,
            disk_quota: 0,
            fingerprint: "license-fingerprint".to_string(),
        }
    }

    fn headers(store_id: Uuid, node_id: &str, fingerprint: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(REPLICATION_NODE_ID_HEADER, node_id.parse().unwrap());
        headers.insert(
            REPLICATION_STORE_ID_HEADER,
            store_id.to_string().parse().unwrap(),
        );
        headers.insert(
            REPLICATION_LICENSE_HASH_HEADER,
            fingerprint.parse().unwrap(),
        );
        headers
    }

    #[fixture]
    fn licensed_guard() -> LicenseDeviceGuard {
        guard(Some(license(1)))
    }

    fn guard(license: Option<License>) -> LicenseDeviceGuard {
        LicenseDeviceGuard::with_receiver_identity(
            license,
            Uuid::nil(),
            "receiver-node".to_string(),
        )
    }

    fn partial_headers(header: axum::http::HeaderName) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header, HeaderValue::from_static("value"));
        headers
    }

    fn empty_node_headers() -> HeaderMap {
        let mut empty = headers(Uuid::new_v4(), "node", "license-fingerprint");
        empty.insert(REPLICATION_NODE_ID_HEADER, HeaderValue::from_static(""));
        empty
    }

    fn non_utf8_node_headers() -> HeaderMap {
        let mut non_utf8 = headers(Uuid::new_v4(), "node", "license-fingerprint");
        non_utf8.insert(
            REPLICATION_NODE_ID_HEADER,
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        non_utf8
    }

    #[rstest]
    #[case::unlicensed(guard(None))]
    #[case::licensed_without_identity(guard(Some(license(1))))]
    fn bypasses_unlicensed_or_identity_free_requests(#[case] guard: LicenseDeviceGuard) {
        assert!(guard.check(&HeaderMap::new()).is_ok());
    }

    #[rstest]
    #[case::node_only(partial_headers(REPLICATION_NODE_ID_HEADER))]
    #[case::store_only(partial_headers(REPLICATION_STORE_ID_HEADER))]
    #[case::license_only(partial_headers(REPLICATION_LICENSE_HASH_HEADER))]
    #[case::empty_node(empty_node_headers())]
    #[case::non_utf8_node(non_utf8_node_headers())]
    fn rejects_invalid_identity_headers(
        licensed_guard: LicenseDeviceGuard,
        #[case] headers: HeaderMap,
    ) {
        assert_eq!(
            licensed_guard
                .validate_headers(&headers)
                .unwrap_err()
                .status,
            ErrorCode::TooManyRequests
        );
    }

    #[rstest]
    #[case::null("null")]
    #[case::different("different-fingerprint")]
    fn rejects_invalid_license_fingerprints(
        licensed_guard: LicenseDeviceGuard,
        #[case] fingerprint: &str,
    ) {
        let invalid = headers(Uuid::new_v4(), "node", fingerprint);
        assert_eq!(
            licensed_guard.check(&invalid).unwrap_err().status,
            ErrorCode::TooManyRequests
        );
    }

    #[rstest]
    fn admits_matching_node_and_enforces_distinct_stores(licensed_guard: LicenseDeviceGuard) {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        assert!(licensed_guard
            .check(&headers(first, "node-a", "license-fingerprint"))
            .is_ok());
        assert!(licensed_guard
            .check(&headers(first, "node-a", "license-fingerprint"))
            .is_ok());
        assert_eq!(
            licensed_guard
                .check(&headers(second, "node-a", "license-fingerprint"))
                .unwrap_err()
                .status,
            ErrorCode::TooManyRequests
        );
        let devices = licensed_guard.devices.lock();
        assert_eq!(devices.len(), 1);
        assert!(!devices.contains_key(&second));
    }

    #[rstest]
    fn refreshes_matching_node_activity(licensed_guard: LicenseDeviceGuard) {
        let store_id = Uuid::new_v4();
        let now = Instant::now();

        licensed_guard
            .check_at(store_id, "node-a".to_string(), now, 1)
            .unwrap();
        licensed_guard
            .check_at(
                store_id,
                "node-a".to_string(),
                now + Duration::from_secs(30),
                1,
            )
            .unwrap();
        assert_eq!(
            licensed_guard
                .check_at(
                    store_id,
                    "node-b".to_string(),
                    now + DEVICE_IDLE_TIMEOUT + Duration::from_secs(1),
                    1,
                )
                .unwrap_err()
                .message,
            ACTIVE_NODE_MESSAGE
        );
    }

    #[rstest]
    fn rejects_different_active_node_without_refreshing_it() {
        let guard = guard(Some(license(1)));
        let store_id = Uuid::new_v4();
        let now = Instant::now();

        guard
            .check_at(store_id, "node-a".to_string(), now, 1)
            .unwrap();
        assert_eq!(
            guard
                .check_at(
                    store_id,
                    "node-b".to_string(),
                    now + Duration::from_secs(30),
                    1,
                )
                .unwrap_err()
                .message,
            ACTIVE_NODE_MESSAGE
        );
        guard
            .check_at(
                store_id,
                "node-b".to_string(),
                now + DEVICE_IDLE_TIMEOUT + Duration::from_secs(1),
                1,
            )
            .unwrap();
        assert_eq!(
            guard
                .check_at(
                    store_id,
                    "node-a".to_string(),
                    now + DEVICE_IDLE_TIMEOUT + Duration::from_secs(2),
                    1,
                )
                .unwrap_err()
                .message,
            ACTIVE_NODE_MESSAGE
        );
    }

    #[rstest]
    fn supports_unlimited_devices_and_releases_expired_stores() {
        let unlimited_guard = guard(Some(license(0)));
        let unlimited_store = Uuid::new_v4();
        assert!(unlimited_guard
            .check(&headers(unlimited_store, "node-a", "license-fingerprint"))
            .is_ok());
        assert!(unlimited_guard
            .check(&headers(unlimited_store, "node-b", "license-fingerprint"))
            .is_ok());
        assert!(unlimited_guard
            .check(&headers(Uuid::new_v4(), "node", "license-fingerprint"))
            .is_ok());

        let guard = guard(Some(license(1)));
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let now = Instant::now();
        guard.check_at(first, "node-a".to_string(), now, 1).unwrap();
        guard
            .check_at(
                second,
                "node-b".to_string(),
                now + DEVICE_IDLE_TIMEOUT + Duration::from_secs(1),
                1,
            )
            .unwrap();
    }

    #[rstest]
    #[case::capped(1)]
    #[case::unlimited(0)]
    fn admits_receiver_identity_without_a_seat_and_rejects_other_nodes(#[case] device_number: u32) {
        let guard = guard(Some(license(device_number)));
        let now = Instant::now();

        guard
            .check_at(Uuid::nil(), "receiver-node".to_string(), now, device_number)
            .unwrap();
        assert!(guard.devices.lock().is_empty());
        assert_eq!(
            guard
                .check_at(Uuid::nil(), "other-node".to_string(), now, device_number,)
                .unwrap_err()
                .message,
            ACTIVE_NODE_MESSAGE
        );
    }
}
