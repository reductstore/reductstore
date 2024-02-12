// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

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
    e: String,
    n: String,
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

    // remove paddings before decoding uses URL_SAFE_NO_PAD
    let license_key = LicenseFile {
        lic: license_key.lic,
        e: license_key.e.replace("=", ""),
        n: license_key.n.replace("=", ""),
    };

    // Parse the license file
    let decoding_key =
        DecodingKey::from_rsa_components(license_key.n.as_str(), license_key.e.as_str());
    if let Err(e) = decoding_key {
        error!("Failed to parse public key: {}", e);
        return None;
    }

    let mut validate = Validation::new(jsonwebtoken::Algorithm::RS256);
    validate.validate_exp = false;
    let license = decode::<License>(license_key.lic.as_str(), &decoding_key.unwrap(), &validate);
    if let Err(e) = license {
        error!("Failed to parse license: {}", e);
        return None;
    }

    Some(license.unwrap().claims)
}
