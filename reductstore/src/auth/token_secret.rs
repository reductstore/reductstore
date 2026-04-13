// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use rand::rngs::SysRng;
use rand::TryRng;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use ring::digest::{digest, SHA256};

const SHA256_PREFIX: &str = "sha256:";
const SALT_LEN: usize = 16;
const HASH_LEN: usize = 32;

pub(crate) fn is_hashed_token_secret(value: &str) -> bool {
    value.starts_with(SHA256_PREFIX)
}

pub(super) fn matched_hashed_token_secret<'a>(stored: &'a str, candidate: &str) -> Option<&'a str> {
    if is_hashed_token_secret(stored) && verify_token_secret(stored, candidate) {
        Some(stored)
    } else {
        None
    }
}

pub(crate) fn hash_token_secret(value: &str) -> Result<String, ReductError> {
    let mut salt = [0u8; SALT_LEN];
    SysRng
        .try_fill_bytes(&mut salt)
        .map_err(|err| internal_server_error!("Failed to generate salt for token hash: {}", err))?;

    let hash = salted_sha256(value, &salt);

    Ok(format!(
        "{}{}:{}",
        SHA256_PREFIX,
        hex::encode(salt),
        hex::encode(hash)
    ))
}

pub(crate) fn verify_token_secret(stored: &str, candidate: &str) -> bool {
    let Some(encoded) = stored.strip_prefix(SHA256_PREFIX) else {
        // Backward compatibility for legacy plaintext tokens.
        return stored == candidate;
    };

    let Some((salt_hex, hash_hex)) = encoded.split_once(':') else {
        return false;
    };

    let salt = match hex::decode(salt_hex) {
        Ok(value) if value.len() == SALT_LEN => value,
        _ => return false,
    };

    let expected_hash = match hex::decode(hash_hex) {
        Ok(value) if value.len() == HASH_LEN => value,
        _ => return false,
    };

    let actual_hash = salted_sha256(candidate, &salt);
    constant_time_eq(expected_hash.as_slice(), &actual_hash)
}

fn salted_sha256(candidate: &str, salt: &[u8]) -> [u8; HASH_LEN] {
    let mut data = Vec::with_capacity(salt.len() + candidate.len());
    data.extend_from_slice(salt);
    data.extend_from_slice(candidate.as_bytes());

    let digest = digest(&SHA256, &data);
    let mut hash = [0u8; HASH_LEN];
    hash.copy_from_slice(digest.as_ref());
    hash
}

// Compare hashes without early exit to avoid leaking mismatch position via timing.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (&a, &b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_and_verify() {
        let secret = "test-secret";
        let hash = hash_token_secret(secret).unwrap();

        assert!(is_hashed_token_secret(&hash));
        assert!(verify_token_secret(&hash, secret));
        assert!(!verify_token_secret(&hash, "wrong-secret"));
    }

    #[test]
    fn test_verify_plaintext_legacy() {
        assert!(verify_token_secret("legacy", "legacy"));
        assert!(!verify_token_secret("legacy", "wrong"));
    }

    #[test]
    fn test_verify_malformed_hash() {
        assert!(!verify_token_secret("sha256:not-a-valid-hash", "secret"));
    }

    #[test]
    fn test_verify_malformed_hash_parts() {
        // invalid salt hex
        assert!(!verify_token_secret("sha256:zzzz:00", "secret"));
        // invalid hash hex
        assert!(!verify_token_secret(
            "sha256:00000000000000000000000000000000:zzzz",
            "secret"
        ));
        // salt must be 16 bytes (32 hex chars)
        assert!(!verify_token_secret(
            "sha256:000000000000000000000000000000:0000000000000000000000000000000000000000000000000000000000000000",
            "secret"
        ));
        // hash must be 32 bytes (64 hex chars)
        assert!(!verify_token_secret(
            "sha256:00000000000000000000000000000000:00000000000000000000000000000000000000000000000000000000000000",
            "secret"
        ));
    }

    #[test]
    fn test_matched_hashed_token_secret() {
        let secret = "test-secret";
        let hash = hash_token_secret(secret).unwrap();

        assert_eq!(
            matched_hashed_token_secret(&hash, secret),
            Some(hash.as_str())
        );
        assert_eq!(matched_hashed_token_secret(&hash, "wrong"), None);
        assert_eq!(matched_hashed_token_secret("legacy", "legacy"), None);
    }

    #[test]
    fn test_constant_time_eq_with_different_lengths() {
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2]));
    }
}
