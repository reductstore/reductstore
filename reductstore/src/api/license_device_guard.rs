// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::deployment_id::StoreId;
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

#[derive(Debug, PartialEq)]
struct ReplicationIdentity {
    node_id: String,
    store_id: String,
    license_hash: String,
}

pub(crate) struct LicenseDeviceGuard {
    license: Option<License>,
    receiver_store_id: Uuid,
    devices: Mutex<HashMap<Uuid, HashMap<String, Instant>>>,
}

impl LicenseDeviceGuard {
    pub(crate) fn new(license: Option<License>, receiver_store_id: StoreId) -> Self {
        Self::with_receiver_store_id(license, receiver_store_id.as_uuid())
    }

    fn with_receiver_store_id(license: Option<License>, receiver_store_id: Uuid) -> Self {
        Self {
            license,
            receiver_store_id,
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
            return Ok(());
        }

        let mut devices = self.devices.lock();
        devices.retain(|_, nodes| {
            nodes.retain(|_, last_seen| now.duration_since(*last_seen) <= DEVICE_IDLE_TIMEOUT);
            !nodes.is_empty()
        });

        if !devices.contains_key(&store_id)
            && device_number != 0
            && devices.len() >= device_number as usize
        {
            return Err(ReductError::new(
                ErrorCode::TooManyRequests,
                DEVICE_LIMIT_MESSAGE,
            ));
        }

        devices.entry(store_id).or_default().insert(node_id, now);
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use chrono::Utc;

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

    fn guard(license: Option<License>) -> LicenseDeviceGuard {
        LicenseDeviceGuard::with_receiver_store_id(license, Uuid::nil())
    }

    #[test]
    fn bypasses_unlicensed_and_identity_free_requests() {
        assert!(guard(None).check(&HeaderMap::new()).is_ok());
        assert!(guard(Some(license(1))).check(&HeaderMap::new()).is_ok());
    }

    #[test]
    fn rejects_partial_or_invalid_identity_headers() {
        let guard = guard(Some(license(1)));
        for header in [
            REPLICATION_NODE_ID_HEADER,
            REPLICATION_STORE_ID_HEADER,
            REPLICATION_LICENSE_HASH_HEADER,
        ] {
            let mut partial = HeaderMap::new();
            partial.insert(header, HeaderValue::from_static("value"));
            assert_eq!(
                guard.validate_headers(&partial).unwrap_err().status,
                ErrorCode::TooManyRequests
            );
        }

        let mut empty = headers(Uuid::new_v4(), "node", "license-fingerprint");
        empty.insert(REPLICATION_NODE_ID_HEADER, HeaderValue::from_static(""));
        assert_eq!(
            guard.validate_headers(&empty).unwrap_err().status,
            ErrorCode::TooManyRequests
        );

        let mut non_utf8 = headers(Uuid::new_v4(), "node", "license-fingerprint");
        non_utf8.insert(
            REPLICATION_NODE_ID_HEADER,
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        assert_eq!(
            guard.validate_headers(&non_utf8).unwrap_err().status,
            ErrorCode::TooManyRequests
        );

        let invalid = headers(Uuid::new_v4(), "node", "null");
        assert_eq!(
            guard.check(&invalid).unwrap_err().status,
            ErrorCode::TooManyRequests
        );
        let invalid = headers(Uuid::new_v4(), "node", "different-fingerprint");
        assert_eq!(
            guard.check(&invalid).unwrap_err().status,
            ErrorCode::TooManyRequests
        );
    }

    #[test]
    fn admits_matching_identity_and_enforces_distinct_stores() {
        let guard = guard(Some(license(1)));
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        assert!(guard
            .check(&headers(first, "node-a", "license-fingerprint"))
            .is_ok());
        assert!(guard
            .check(&headers(first, "node-b", "license-fingerprint"))
            .is_ok());
        assert_eq!(
            guard
                .check(&headers(second, "node-a", "license-fingerprint"))
                .unwrap_err()
                .status,
            ErrorCode::TooManyRequests
        );
    }

    #[test]
    fn supports_unlimited_devices_and_releases_expired_stores() {
        let unlimited_guard = guard(Some(license(0)));
        assert!(unlimited_guard
            .check(&headers(Uuid::new_v4(), "node", "license-fingerprint"))
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
}
