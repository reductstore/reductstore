// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::zenoh::{ZenohBlock, ZenohBucketRouting};
use crate::storage::entry::matches_patterns;
use std::fmt::{Display, Formatter};

const MAX_BUCKET_NAME_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoutingMode {
    Static {
        bucket: String,
    },
    /// Single-chunk keys use `fallback`; without one they are rejected.
    KeyPrefix {
        fallback: Option<String>,
    },
}

/// Routing and authorisation policy of one configured block.
#[derive(Debug, Clone)]
pub(crate) struct BucketRouter {
    block_id: String,
    mode: RoutingMode,
    allowlist: Vec<String>,
    allow_bucket_creation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Route<'a> {
    pub bucket: &'a str,
    pub entry: &'a str,
    pub may_create: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoutingError {
    EmptyKey,
    NoFallbackBucket { key: String },
    NotAllowed { bucket: String },
    InvalidBucketName { bucket: String, reason: String },
    InvalidEntryName { entry: String },
    BucketMissing { bucket: String },
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::EmptyKey => write!(f, "Key expression is empty"),
            RoutingError::NoFallbackBucket { key } => write!(
                f,
                "Key '{}' has no prefix chunk to select a bucket and the block has no fallback bucket",
                key
            ),
            RoutingError::NotAllowed { bucket } => {
                write!(f, "Bucket '{}' is not in the block's allowlist", bucket)
            }
            RoutingError::InvalidBucketName { bucket, reason } => {
                write!(f, "Bucket name '{}' {}", bucket, reason)
            }
            RoutingError::InvalidEntryName { entry } => {
                write!(f, "Entry name '{}' is not valid", entry)
            }
            RoutingError::BucketMissing { bucket } => write!(
                f,
                "Bucket '{}' does not exist and the block may not create it",
                bucket
            ),
        }
    }
}

impl RoutingError {
    /// Message sent back to a remote querier; bucket names and policy details stay local.
    pub(crate) fn public_message(&self) -> &'static str {
        match self {
            RoutingError::EmptyKey | RoutingError::InvalidEntryName { .. } => {
                "Key expression cannot be resolved to an entry"
            }
            _ => "Bucket is not available over this queryable",
        }
    }
}

impl BucketRouter {
    pub(crate) fn from_block(block: &ZenohBlock) -> Self {
        let mode = match block.routing {
            ZenohBucketRouting::Static => RoutingMode::Static {
                // a static block without a bucket is dropped while parsing
                bucket: block.bucket.clone().unwrap_or_default(),
            },
            ZenohBucketRouting::KeyPrefix => RoutingMode::KeyPrefix {
                fallback: block.bucket.clone(),
            },
        };

        BucketRouter {
            block_id: block.id.clone(),
            mode,
            allowlist: block.bucket_allowlist.clone(),
            allow_bucket_creation: block.allow_bucket_creation,
        }
    }

    pub(crate) fn block_id(&self) -> &str {
        &self.block_id
    }

    pub(crate) fn declared_bucket(&self) -> Option<&str> {
        match &self.mode {
            RoutingMode::Static { bucket } => Some(bucket),
            RoutingMode::KeyPrefix { fallback } => fallback.as_deref(),
        }
    }

    pub(crate) fn resolve<'a>(&'a self, key_expr: &'a str) -> Result<Route<'a>, RoutingError> {
        let key = key_expr.trim_matches('/');
        if key.is_empty() {
            return Err(RoutingError::EmptyKey);
        }

        let (bucket, entry, derived) = match &self.mode {
            RoutingMode::Static { bucket } => (bucket.as_str(), key, false),
            RoutingMode::KeyPrefix { fallback } => match key.split_once('/') {
                Some((bucket, entry)) if !bucket.is_empty() && !entry.is_empty() => {
                    (bucket, entry, true)
                }
                _ => match fallback {
                    Some(bucket) => (bucket.as_str(), key, false),
                    None => {
                        return Err(RoutingError::NoFallbackBucket {
                            key: key.to_string(),
                        })
                    }
                },
            },
        };

        if derived {
            validate_bucket_name(bucket)?;
        }
        validate_entry_name(entry)?;

        if !self.bucket_allowed(bucket) {
            return Err(RoutingError::NotAllowed {
                bucket: bucket.to_string(),
            });
        }

        Ok(Route {
            bucket,
            entry,
            may_create: derived && self.allow_bucket_creation,
        })
    }

    pub(crate) fn validate(&self) -> Result<(), RoutingError> {
        let Some(bucket) = self.declared_bucket() else {
            return Ok(());
        };

        validate_bucket_name(bucket)?;
        if !self.bucket_allowed(bucket) {
            return Err(RoutingError::NotAllowed {
                bucket: bucket.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn config_warnings(&self) -> Vec<String> {
        self.allowlist
            .iter()
            .filter(|pattern| pattern.contains('/'))
            .map(|pattern| {
                format!(
                    "allowlist pattern '{}' contains '/' and can never match a bucket name",
                    pattern
                )
            })
            .collect()
    }

    pub(crate) fn describe(&self) -> String {
        let allowlist = if self.allowlist.is_empty() {
            "<any>".to_string()
        } else {
            self.allowlist.join(",")
        };

        match &self.mode {
            RoutingMode::Static { bucket } => format!("routing=static bucket='{}'", bucket),
            RoutingMode::KeyPrefix { fallback } => format!(
                "routing=key-prefix fallback={} allowlist=[{}] allow_bucket_creation={}",
                fallback
                    .as_deref()
                    .map(|bucket| format!("'{}'", bucket))
                    .unwrap_or_else(|| "<none>".to_string()),
                allowlist,
                self.allow_bucket_creation
            ),
        }
    }

    fn bucket_allowed(&self, bucket: &str) -> bool {
        self.allowlist.is_empty() || matches_patterns(bucket, &self.allowlist)
    }
}

fn validate_bucket_name(name: &str) -> Result<(), RoutingError> {
    let reject = |reason: &str| {
        Err(RoutingError::InvalidBucketName {
            bucket: name.to_string(),
            reason: reason.to_string(),
        })
    };

    if name.is_empty() {
        return reject("is empty");
    }
    if name.len() > MAX_BUCKET_NAME_LEN {
        return reject(&format!(
            "is longer than {} characters",
            MAX_BUCKET_NAME_LEN
        ));
    }
    // rejecting '$' keeps a key expression from reaching a system bucket
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return reject("may contain only letters, digits and [-,_] symbols");
    }
    Ok(())
}

fn validate_entry_name(entry: &str) -> Result<(), RoutingError> {
    if entry.is_empty() || entry.split('/').any(|segment| segment.is_empty()) {
        return Err(RoutingError::InvalidEntryName {
            entry: entry.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn block(routing: ZenohBucketRouting, bucket: Option<&str>, allowlist: &[&str]) -> ZenohBlock {
        ZenohBlock {
            id: "0".to_string(),
            keyexprs: vec!["**".to_string()],
            routing,
            bucket: bucket.map(|name| name.to_string()),
            bucket_allowlist: allowlist.iter().map(|p| p.to_string()).collect(),
            allow_bucket_creation: false,
        }
    }

    fn key_prefix(bucket: Option<&str>, allowlist: &[&str], allow_creation: bool) -> BucketRouter {
        let mut block = block(ZenohBucketRouting::KeyPrefix, bucket, allowlist);
        block.allow_bucket_creation = allow_creation;
        BucketRouter::from_block(&block)
    }

    fn static_router(bucket: &str) -> BucketRouter {
        BucketRouter::from_block(&block(ZenohBucketRouting::Static, Some(bucket), &[]))
    }

    #[rstest]
    #[case(
        "run_abc/motion/welder/commanded",
        "run_abc",
        "motion/welder/commanded"
    )]
    #[case("/run_abc/lifecycle", "run_abc", "lifecycle")]
    #[case("wc_cell/run_started/", "wc_cell", "run_started")]
    fn key_prefix_splits_first_chunk(#[case] key: &str, #[case] bucket: &str, #[case] entry: &str) {
        let router = key_prefix(None, &["*"], true);
        let route = router.resolve(key).unwrap();
        assert_eq!(route.bucket, bucket);
        assert_eq!(route.entry, entry);
        assert!(route.may_create);
    }

    #[rstest]
    #[case("orphan")]
    #[case("/orphan/")]
    fn key_prefix_uses_fallback_for_single_chunk(#[case] key: &str) {
        let router = key_prefix(Some("zenoh"), &["*"], true);
        let route = router.resolve(key).unwrap();
        assert_eq!(route.bucket, "zenoh");
        assert_eq!(route.entry, "orphan");
        assert!(!route.may_create);
    }

    #[rstest]
    fn key_prefix_without_fallback_rejects_single_chunk() {
        let router = key_prefix(None, &["*"], true);
        assert_eq!(
            router.resolve("orphan"),
            Err(RoutingError::NoFallbackBucket {
                key: "orphan".to_string()
            })
        );
    }

    #[rstest]
    fn static_uses_full_key_as_entry() {
        let router = static_router("bucket-1");
        let route = router.resolve("/factory/line1/status").unwrap();
        assert_eq!(route.bucket, "bucket-1");
        assert_eq!(route.entry, "factory/line1/status");
        assert!(!route.may_create);
    }

    #[rstest]
    #[case("")]
    #[case("/")]
    #[case("///")]
    fn rejects_empty_key(#[case] key: &str) {
        assert_eq!(
            static_router("bucket-1").resolve(key),
            Err(RoutingError::EmptyKey)
        );
    }

    #[rstest]
    #[case("$system/evil/entry")]
    #[case("../etc/passwd")]
    #[case("bad.name/entry")]
    fn rejects_unsafe_derived_bucket_name(#[case] key: &str) {
        let err = key_prefix(None, &["*"], true).resolve(key).unwrap_err();
        assert!(matches!(err, RoutingError::InvalidBucketName { .. }));
    }

    #[rstest]
    fn rejects_over_long_derived_bucket_name() {
        let key = format!("{}/entry", "a".repeat(MAX_BUCKET_NAME_LEN + 1));
        let err = key_prefix(None, &["*"], true).resolve(&key).unwrap_err();
        assert!(matches!(err, RoutingError::InvalidBucketName { .. }));
    }

    #[rstest]
    fn accepts_bucket_name_at_the_length_limit() {
        let name = "a".repeat(MAX_BUCKET_NAME_LEN);
        let key = format!("{}/entry", name);
        let router = key_prefix(None, &["*"], true);
        let route = router.resolve(&key).unwrap();
        assert_eq!(route.bucket, name);
    }

    #[rstest]
    #[case("bucket-1//entry")]
    #[case("a//b")]
    fn rejects_entry_with_empty_segments(#[case] key: &str) {
        let err = key_prefix(None, &["*"], true).resolve(key).unwrap_err();
        assert!(matches!(err, RoutingError::InvalidEntryName { .. }));
    }

    #[rstest]
    #[case("site_a/temp", true)]
    #[case("cell_7/temp", true)]
    #[case("other/temp", false)]
    fn allowlist_filters_derived_buckets(#[case] key: &str, #[case] allowed: bool) {
        let router = key_prefix(None, &["site_*", "cell_*"], true);
        assert_eq!(router.resolve(key).is_ok(), allowed);
    }

    #[rstest]
    fn allowlist_supports_exclusions() {
        let router = key_prefix(None, &["site_*", "!site_test*"], true);
        assert!(router.resolve("site_prod/temp").is_ok());
        assert_eq!(
            router.resolve("site_test/temp"),
            Err(RoutingError::NotAllowed {
                bucket: "site_test".to_string()
            })
        );
    }

    #[rstest]
    fn may_create_is_false_when_creation_is_disabled() {
        let router = key_prefix(None, &["site_*"], false);
        assert!(!router.resolve("site_a/temp").unwrap().may_create);
    }

    #[rstest]
    fn validate_accepts_a_declared_bucket_inside_its_allowlist() {
        assert!(key_prefix(Some("site_fallback"), &["site_*"], true)
            .validate()
            .is_ok());
        assert!(static_router("bucket-1").validate().is_ok());
    }

    #[rstest]
    fn validate_rejects_a_declared_bucket_outside_its_allowlist() {
        assert_eq!(
            key_prefix(Some("zenoh_misc"), &["site_*"], true).validate(),
            Err(RoutingError::NotAllowed {
                bucket: "zenoh_misc".to_string()
            })
        );
    }

    #[rstest]
    fn validate_rejects_a_system_bucket() {
        let err = static_router("$system").validate().unwrap_err();
        assert!(matches!(err, RoutingError::InvalidBucketName { .. }));
    }

    #[rstest]
    fn config_warnings_flag_key_expression_patterns() {
        let warnings = key_prefix(None, &["site_*", "site_$*/**"], true).config_warnings();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("site_$*/**"));
    }

    #[rstest]
    fn public_message_hides_bucket_names() {
        let err = RoutingError::NotAllowed {
            bucket: "secret_bucket".to_string(),
        };
        assert!(!err.public_message().contains("secret_bucket"));
    }
}
