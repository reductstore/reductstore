// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use hex::ToHex;
use jsonwebtoken::{decode, DecodingKey, Validation};
use log::error;
use reduct_base::msg::server_api::License;
use serde_json::de;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

#[derive(Debug, serde::Deserialize)]
struct LicenseFile {
    lic: String,
    key: String,
}

pub(crate) fn parse_license(license_path: Option<String>) -> Option<License> {
    if license_path.is_none() {
        return None;
    }

    let license_path = license_path.unwrap();
    let file = std::fs::read(&license_path);
    if let Err(e) = file {
        error!("Failed to read license file: {}", e);
        return None;
    }

    let license_key = de::from_slice::<LicenseFile>(&file.unwrap());
    if let Err(e) = license_key {
        error!("Failed to parse license file {}: {}", license_path, e);
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

    let mut license = license.unwrap().claims;
    license.fingerprint =
        ring::digest::digest(&ring::digest::SHA256, license_key.lic.as_bytes()).encode_hex();
    Some(license)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[rstest]
    fn test_parse_license(license_file: PathBuf) {
        let license = parse_license(Some(license_file.to_str().unwrap().to_string()));
        assert!(license.is_some());
        let license = license.unwrap();
        assert_eq!(license.licensee, "ReductStore LLC");
        assert_eq!(license.invoice, "001");
        assert_eq!(license.plan, "STANDARD");
        assert_eq!(license.device_number, 1);
        assert_eq!(license.disk_quota, 5);
        assert_eq!(
            license.fingerprint,
            "eacc62490ac0968adacf0f8860032942d63f4c2443519336ae1ee5ae8e31708c"
        );
    }

    #[rstest]
    fn test_parse_bad_license(bad_license_file: PathBuf) {
        let license = parse_license(Some(bad_license_file.to_str().unwrap().to_string()));
        assert!(license.is_none());
    }

    #[fixture]
    fn license_file() -> PathBuf {
        let license_file = r#"
        {
            "lic": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJsaWNlbnNlZSI6IlJlZHVjdFN0b3JlIExMQyIsImludm9pY2UiOiIwMDEiLCJkaXNrX3F1b3RhIjo1LCJwbGFuIjoiU1RBTkRBUkQiLCJkZXZpY2VfbnVtYmVyIjoxLCJleHBpcnlfZGF0ZSI6IjIwMjUtMDEtMDFUMDA6MDA6MDArMDA6MDAiLCJleHAiOjB9.TMwVPmrKSpSlRNw_3Vrx3-mDqnrRTqLf0wo9lLYfmhiN_vI5F8xxXa-SlHLgSkSSBc5LvVloI4X0TuQd_6IJy6t39bO5zbTKk1tynPs_25jVpPpF3ZBifOv_yvkAM3INCi3D2Xr9cUUW5pn8Ewc3_VegGnuGf4ayfOaxLd_NiSiTOkOXetj-oXSq23V1PKwx60S2-TZ9BXR4w2z9IRKgEOCGMHhm6ng6dAGIcnL75NjesCSF2GKQuusdlKoF7ezdVy8Y0-CUtKG4ieA5dRCTd8-ojOKNaEmq8Dtwev168dE9loCKbhkz3GyMHHAs-OzPijsNDTPL1RLCNIJ1Qar4Vg",
            "key": "MIIBCgKCAQEAtE7uXxVOAGb02kprK7NHNmex8SLoiPtOryEbBewe5QGkI7/J7TG6jy5sPmssvgPgzNQMz6gB41YcefiO/ndzkVAO7TwfoEhTT+9xhB3N9ggdzVmhuiYrkqMWmfJgqH2J4L5Q1NxQyzBHJG/fSk0/OiBqEvbZkZ0jURBu9chEf9UskRu7y9d9wzuXPxQiGU/KAWSFXbRu7bwB+/o5dD9V6srU5bNjRt/gNMPb/+Wm6iyKfWlxYGbfFYjWyWOHzVZFQqWiJNAszsCsZ4HDtV+iZaWhc5pnoYVcPGlwpN7gTwKI6OogcT2701/X/NlN/oeZS7SAseeiGCFyngR4e5vzGQIDAQAB"
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
