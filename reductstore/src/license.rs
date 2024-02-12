// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use jsonwebtoken::{decode, DecodingKey, Validation};
use log::error;
use serde_json::de;
use std::path::PathBuf;

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

pub(crate) fn parse_license(license_path: PathBuf) -> Option<License> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::{tempdir, tempfile};

    #[rstest]
    fn test_parse_license(license_file: PathBuf) {
        let license = parse_license(license_file);
        assert!(license.is_some());
        let license = license.unwrap();
        assert_eq!(license.licensee, "ReductStore LLC");
        assert_eq!(license.invoice, "001");
        assert_eq!(license.plan, "STANDARD");
        assert_eq!(license.device_number, 1);
        assert_eq!(license.disk_space, 5);
    }

    #[rstest]
    fn test_parse_bad_license(bad_license_file: PathBuf) {
        let license = parse_license(bad_license_file);
        assert!(license.is_none());
    }

    #[fixture]
    fn license_file() -> PathBuf {
        let license_file = r#"
        {
            "lic": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJsaWNlbnNlZSI6IlJlZHVjdFN0b3JlIExMQyIsImludm9pY2UiOiIwMDEiLCJkaXNrX3NwYWNlIjo1LCJwbGFuIjoiU1RBTkRBUkQiLCJkZXZpY2VfbnVtYmVyIjoxLCJleHBpcnlfZGF0ZSI6IjIwMjUtMDEtMDFUMDA6MDA6MDArMDA6MDAiLCJleHAiOjB9.A5aTXgF286R8hm-9uz18pGgVu_XAYWV7tFREMsvmVXf-8OCdxq6EAiUg5fkthDj2Ih7PXxi6q41dyqJdh6LC5VPOyMTa1b-WvybG9IEWjSW1KYmB8IH6taphl60L1DygAMSSIFehLJiDIfvezuSHj_m9djGuSHJ2OGD-nI78beV8heW8ol91qiJgGdgEyxgmaSS9BQuCToZqEhUyZk8VZcpGUrcKzWcW2NmTKjn_7qNEk4FF9Eg5GtSKnNZ-LdOwGGAwPlIW9UB-nQkcsb1cNmUchHzkI_jnAAbth3L_WFEscIqOgUIGmEv5ryEo71Xta_wCiFHgyU2qgAV_zbITGg",
            "key": "MIIBCgKCAQEAwzKW/myo07w03DI9mHLZIK7r2Vn2LzWKtrL/elxUuBegt9YEudrldn9+esusuD0vdwjT9t81OkBvb6KvO05Hd93Q4IhzMvNRNl96iquWAQCyYN6Iob2NV0TsNEzbhnEoyELqVUdHlFxmRvcuJ1p8QAvN0f4hetj2PvPlYRCBDp9N/0kkUoJE/9FHIB+I2L0EOtUI1gAZUr3KX7YvEKtpz0Di6u1dqPo17AZDgYm3p42Yqg+jK2lGRH0WjLk1IzQ5x4zxJK3trUl+xyGRLpv+Sxv2V/B5wHuTw3/hEOXzLt/X7/wOy1O7F3eWNHoWjunJHY3NgM/s8BfTGr2QQkyYGwIDAQAB"
        }
        "#;

        let license_path = tempdir().unwrap().into_path().join("license.jwt");
        fs::write(&license_path, license_file).unwrap();
        license_path
    }

    #[fixture]
    fn bad_license_file() -> PathBuf {
        let license_file = r#"
        {
            "lic": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.BADeyJsaWNlbnNlZSI6IlJlZHVjdFN0b3JlIExMQyIsImludm9pY2UiOiIwMDEiLCJkaXNrX3NwYWNlIjo1LCJwbGFuIjoiU1RBTkRBUkQiLCJkZXZpY2VfbnVtYmVyIjoxLCJleHBpcnlfZGF0ZSI6IjIwMjUtMDEtMDFUMDA6MDA6MDArMDA6MDAiLCJleHAiOjB9.A5aTXgF286R8hm-9uz18pGgVu_XAYWV7tFREMsvmVXf-8OCdxq6EAiUg5fkthDj2Ih7PXxi6q41dyqJdh6LC5VPOyMTa1b-WvybG9IEWjSW1KYmB8IH6taphl60L1DygAMSSIFehLJiDIfvezuSHj_m9djGuSHJ2OGD-nI78beV8heW8ol91qiJgGdgEyxgmaSS9BQuCToZqEhUyZk8VZcpGUrcKzWcW2NmTKjn_7qNEk4FF9Eg5GtSKnNZ-LdOwGGAwPlIW9UB-nQkcsb1cNmUchHzkI_jnAAbth3L_WFEscIqOgUIGmEv5ryEo71Xta_wCiFHgyU2qgAV_zbITGg",
            "key": "MIIBCgKCAQEAwzKW/myo07w03DI9mHLZIK7r2Vn2LzWKtrL/elxUuBegt9YEudrldn9+esusuD0vdwjT9t81OkBvb6KvO05Hd93Q4IhzMvNRNl96iquWAQCyYN6Iob2NV0TsNEzbhnEoyELqVUdHlFxmRvcuJ1p8QAvN0f4hetj2PvPlYRCBDp9N/0kkUoJE/9FHIB+I2L0EOtUI1gAZUr3KX7YvEKtpz0Di6u1dqPo17AZDgYm3p42Yqg+jK2lGRH0WjLk1IzQ5x4zxJK3trUl+xyGRLpv+Sxv2V/B5wHuTw3/hEOXzLt/X7/wOy1O7F3eWNHoWjunJHY3NgM/s8BfTGr2QQkyYGwIDAQAB"
        }
        "#;

        let license_path = tempdir().unwrap().into_path().join("license.jwt");
        fs::write(&license_path, license_file).unwrap();
        license_path
    }
}
