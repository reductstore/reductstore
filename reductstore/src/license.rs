// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytesize::ByteSize;
use chrono::{Date, DateTime, NaiveDate, Utc};
use jsonwebtoken::{decode, DecodingKey, Validation};
use log::error;
use serde_json::de;

/// A license for ReductStore.
///
#[derive(Debug, serde::Deserialize)]
pub(crate) struct License {
    licensee: String,
    invoice: String,
    expiry_date: DateTime<Utc>,
    plan: String,
    device_number: u32,
    disk_space: i32,
}

#[derive(Debug, serde::Deserialize)]
struct LicenseFile {
    lic: String,
    key: String,
}

pub(crate) fn parse_license(license_path: &str) -> Option<License> {
    let file = std::fs::read(license_path);
    if let Err(e) = file {
        error!("Failed to read license file: {}", e);
        return None;
    }

    let license_key = de::from_slice::<LicenseFile>(&file.unwrap());
    if let Err(e) = license_key {
        error!("Failed to parse license file: {}", e);
        return None;
    }
    let license_key = license_key.unwrap();

    // Parse the license file
    let encoded_key = BASE64_STANDARD.decode(license_key.key.as_str());
    if let Err(e) = encoded_key {
        error!("Failed to decode license key: {}", e);
        return None;
    }
    let decoding_key = DecodingKey::from_rsa_der(encoded_key.unwrap().as_slice());

    let mut validate = Validation::new(jsonwebtoken::Algorithm::RS256);
    validate.validate_exp = false;
    let license = decode::<License>(license_key.lic.as_str(), &decoding_key, &validate);
    if let Err(e) = license {
        error!("Failed to parse license: {}", e);
        return None;
    }

    Some(license.unwrap().claims)
}
