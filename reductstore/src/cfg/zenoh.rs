// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{parse_bool, CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use log::{debug, error, info, warn};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

const DEFAULT_BUCKET: &str = "zenoh";

const LEGACY_BLOCK_ID: &str = "legacy";

const SUB_PREFIX: &str = "RS_ZENOH_SUB_";
const QUERY_PREFIX: &str = "RS_ZENOH_QUERY_";

const BLOCK_SUFFIXES: [&str; 6] = [
    "KEYEXPRS",
    "ROUTING",
    "BUCKET",
    "BUCKET_ALLOWLIST",
    "ALLOW_BUCKET_CREATION",
    "LOCALITY",
];

const LEGACY_KEYS: [&str; 4] = [
    "RS_ZENOH_BUCKET",
    "RS_ZENOH_SUB_KEYEXPRS",
    "RS_ZENOH_QUERY_KEYEXPRS",
    "RS_ZENOH_QUERY_LOCALITY",
];

/// Configuration for the Zenoh API integration.
///
/// # Global Environment Variables
///
/// One Zenoh session is opened for the whole instance, so transport settings are not per block:
///
/// - `RS_ZENOH_ENABLED`: Enable/disable the Zenoh integration (default: false)
/// - `RS_ZENOH_CONFIG`: Inline Zenoh config string (e.g., "mode=client;peer=localhost:7447")
/// - `RS_ZENOH_CONFIG_PATH`: Path to a Zenoh JSON5 config file
///
/// If both `RS_ZENOH_CONFIG` and `RS_ZENOH_CONFIG_PATH` are set, inline config takes precedence.
///
/// ## Inline Credential Files (for cloud environments)
///
/// When using `RS_ZENOH_CONFIG`, the Zenoh config can reference credential file paths.
/// These env vars allow providing file content inline, which is written to temp files:
///
/// - `RS_ZENOH_TLS_ROOT_CA`: Inline root CA certificate (for `transport/link/tls/root_ca_certificate`)
/// - `RS_ZENOH_TLS_CONNECT_CERT`: Inline mTLS client certificate (for `transport/link/tls/connect_certificate`)
/// - `RS_ZENOH_TLS_CONNECT_KEY`: Inline mTLS client private key (for `transport/link/tls/connect_private_key`)
/// - `RS_ZENOH_AUTH_DICTIONARY`: Inline auth dictionary content (for `transport/auth/usrpwd/dictionary_file`)
///
/// # Block Environment Variables
///
/// The write path is configured as `RS_ZENOH_SUB_<ID>_*` blocks and the read path as
/// `RS_ZENOH_QUERY_<ID>_*` blocks. `<ID>` is any token; numeric ids are listed first.
/// A block declares one or more key expressions and the bucket policy applied to them.
///
/// - `RS_ZENOH_{SUB,QUERY}_<ID>_KEYEXPRS`: Zenoh key expressions, comma separated (required)
/// - `RS_ZENOH_{SUB,QUERY}_<ID>_ROUTING`: `static` (default) or `key-prefix`
/// - `RS_ZENOH_{SUB,QUERY}_<ID>_BUCKET`: in `static` routing, the target bucket (required);
///   in `key-prefix` routing, an optional fallback for keys with a single chunk
/// - `RS_ZENOH_{SUB,QUERY}_<ID>_BUCKET_ALLOWLIST`: comma-separated globs limiting which bucket
///   names a key prefix may resolve to. Required in `key-prefix` routing, ignored in `static`
/// - `RS_ZENOH_SUB_<ID>_ALLOW_BUCKET_CREATION`: whether `key-prefix` routing may create buckets
///   on demand (default: false). Ignored in `static` routing, where the named bucket is always
///   created at startup. Queryables never create buckets
/// - `RS_ZENOH_QUERY_<ID>_LOCALITY`: allowed origin for query replies. One of `SessionLocal`,
///   `Remote`, or `Any` (default)
///
/// In `key-prefix` routing the first chunk of the key selects the bucket and the rest becomes
/// the entry name, e.g. `site_a/motion/welder` -> bucket `site_a`, entry `motion/welder`.
/// `_BUCKET_ALLOWLIST` takes plain globs (`*`, leading `!` excludes), not Zenoh key expressions.
///
/// # Legacy (unindexed) Configuration
///
/// `RS_ZENOH_BUCKET` (default: "zenoh"), `RS_ZENOH_SUB_KEYEXPRS`, `RS_ZENOH_QUERY_KEYEXPRS`
/// and `RS_ZENOH_QUERY_LOCALITY` configure a single static block writing everything to one
/// bucket. They apply only when no indexed variable is set; otherwise they are ignored.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ZenohApiConfig {
    /// Enables the Zenoh API runtime.
    pub enabled: bool,
    /// Inline Zenoh configuration string (e.g., "mode=client;peer=localhost:7447").
    /// Takes precedence over `config_path` if both are set.
    pub config_inline: Option<String>,
    /// Path to a Zenoh JSON5 configuration file.
    pub config_path: Option<String>,
    /// Write-path blocks. Empty disables the write path.
    pub subscribers: Vec<ZenohBlock>,
    /// Read-path blocks. Empty disables the read path.
    pub queryables: Vec<ZenohQueryableBlock>,
    /// Inline root CA certificate content.
    /// Written to a temp file and injected as `transport/link/tls/root_ca_certificate`.
    pub tls_root_ca_cert: Option<String>,
    /// Inline mTLS client certificate content.
    /// Written to a temp file and injected as `transport/link/tls/connect_certificate`.
    pub tls_connect_cert: Option<String>,
    /// Inline mTLS client private key content.
    /// Written to a temp file and injected as `transport/link/tls/connect_private_key`.
    pub tls_connect_key: Option<String>,
    /// Inline user/password dictionary content (user:password per line).
    /// Written to a temp file and injected as `transport/auth/usrpwd/dictionary_file`.
    pub auth_dictionary: Option<String>,
}

/// A set of Zenoh key expressions and the bucket policy applied to keys matching them.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ZenohBlock {
    /// The `<ID>` from the environment variable names.
    pub id: String,
    /// Zenoh key expressions.
    pub keyexprs: Vec<String>,
    /// How keys matching this block are mapped to buckets.
    pub routing: ZenohBucketRouting,
    /// Target bucket in `Static` routing, optional fallback for single-chunk keys in `KeyPrefix`.
    pub bucket: Option<String>,
    /// Globs limiting which bucket names a key prefix may resolve to.
    pub bucket_allowlist: Vec<String>,
    /// Whether `KeyPrefix` routing may create buckets on demand.
    pub allow_bucket_creation: bool,
}

impl ZenohBlock {
    pub fn is_key_prefix(&self) -> bool {
        self.routing == ZenohBucketRouting::KeyPrefix
    }
}

/// A read-path block: the routing policy plus the queryable's allowed reply origin.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ZenohQueryableBlock {
    pub route: ZenohBlock,
    pub locality: ZenohQueryableLocality,
}

/// How Zenoh keys are mapped to ReductStore buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ZenohBucketRouting {
    #[default]
    Static,
    KeyPrefix,
}

impl Display for ZenohBucketRouting {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            ZenohBucketRouting::Static => "static",
            ZenohBucketRouting::KeyPrefix => "key-prefix",
        };
        write!(f, "{}", value)
    }
}

impl FromStr for ZenohBucketRouting {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "static" => Ok(ZenohBucketRouting::Static),
            "key-prefix" | "key_prefix" | "keyprefix" => Ok(ZenohBucketRouting::KeyPrefix),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ZenohQueryableLocality {
    SessionLocal,
    Remote,
    #[default]
    Any,
}

impl Display for ZenohQueryableLocality {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            ZenohQueryableLocality::SessionLocal => "SessionLocal",
            ZenohQueryableLocality::Remote => "Remote",
            ZenohQueryableLocality::Any => "Any",
        };
        write!(f, "{}", value)
    }
}

impl FromStr for ZenohQueryableLocality {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "sessionlocal" => Ok(ZenohQueryableLocality::SessionLocal),
            "remote" => Ok(ZenohQueryableLocality::Remote),
            "any" => Ok(ZenohQueryableLocality::Any),
            _ => Err(()),
        }
    }
}

impl Display for ZenohBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: keyexprs={}, routing={}, bucket={}",
            self.id,
            self.keyexprs.join(","),
            self.routing,
            self.bucket.as_deref().unwrap_or("<none>")
        )?;

        if self.is_key_prefix() {
            write!(
                f,
                ", allowlist={}, allow_bucket_creation={}",
                if self.bucket_allowlist.is_empty() {
                    "<any>".to_string()
                } else {
                    self.bucket_allowlist.join(",")
                },
                self.allow_bucket_creation
            )?;
        }

        Ok(())
    }
}

impl Display for ZenohQueryableBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, locality={}", self.route, self.locality)
    }
}

impl Display for ZenohApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, config={}, config_path={}, tls={}, auth={}, subscribers={}, queryables={}",
            self.enabled,
            self.config_inline.as_deref().unwrap_or("<none>"),
            self.config_path.as_deref().unwrap_or("<none>"),
            self.tls_root_ca_cert.is_some() || self.tls_connect_cert.is_some(),
            self.auth_dictionary.is_some(),
            format_blocks(&self.subscribers),
            format_blocks(&self.queryables)
        )
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_zenoh_api_config(env: &mut Env<EnvGetter>) -> ZenohApiConfig {
        let zenoh_keys = env.keys_with_prefix("RS_ZENOH_");
        let indexed_keys: Vec<&String> = zenoh_keys
            .iter()
            .filter(|key| is_indexed_block_key(key))
            .collect();
        let legacy_keys: Vec<&String> = zenoh_keys
            .iter()
            .filter(|key| LEGACY_KEYS.contains(&key.as_str()))
            .collect();

        let (subscribers, queryables) = if indexed_keys.is_empty() {
            parse_legacy_blocks(env)
        } else {
            if !legacy_keys.is_empty() {
                warn!(
                    "Zenoh: indexed settings are configured ({}), so the unindexed settings ({}) are ignored",
                    join_keys(&indexed_keys),
                    join_keys(&legacy_keys)
                );
            }
            (
                parse_blocks(env, SUB_PREFIX, "subscriber"),
                parse_queryables(env),
            )
        };

        let config = ZenohApiConfig {
            enabled: parse_bool(env.get_optional::<String>("RS_ZENOH_ENABLED"), false),
            config_inline: parse_optional_string(env.get_optional::<String>("RS_ZENOH_CONFIG")),
            config_path: parse_optional_string(env.get_optional::<String>("RS_ZENOH_CONFIG_PATH")),
            subscribers,
            queryables,
            tls_root_ca_cert: parse_optional_string(
                env.get_optional::<String>("RS_ZENOH_TLS_ROOT_CA"),
            ),
            tls_connect_cert: parse_optional_string(
                env.get_optional::<String>("RS_ZENOH_TLS_CONNECT_CERT"),
            ),
            tls_connect_key: parse_optional_string(
                env.get_optional::<String>("RS_ZENOH_TLS_CONNECT_KEY"),
            ),
            auth_dictionary: parse_optional_string(
                env.get_optional::<String>("RS_ZENOH_AUTH_DICTIONARY"),
            ),
        };

        if config.enabled && config.subscribers.is_empty() && config.queryables.is_empty() {
            warn!(
                "Zenoh API is enabled but no subscriber or queryable block is configured. \
                 Set RS_ZENOH_SUB_0_KEYEXPRS and/or RS_ZENOH_QUERY_0_KEYEXPRS"
            );
        }

        config
    }
}

fn parse_queryables<EnvGetter: GetEnv>(env: &mut Env<EnvGetter>) -> Vec<ZenohQueryableBlock> {
    parse_blocks(env, QUERY_PREFIX, "queryable")
        .into_iter()
        .map(|route| {
            let locality = parse_locality(
                env.get_optional::<String>(&format!("{}{}_LOCALITY", QUERY_PREFIX, route.id)),
                &format!("queryable[{}]", route.id),
            );
            ZenohQueryableBlock { route, locality }
        })
        .collect()
}

fn parse_blocks<EnvGetter: GetEnv>(
    env: &mut Env<EnvGetter>,
    prefix: &str,
    kind: &str,
) -> Vec<ZenohBlock> {
    let discovered = env.matches::<String>(&format!("^{}(.+)_KEYEXPRS$", prefix));
    let discovered_ids: Vec<String> = discovered.keys().cloned().collect();

    let mut blocks = Vec::with_capacity(discovered.len());
    for (id, raw_keyexprs) in discovered {
        let label = format!("{}[{}]", kind, id);

        let keyexprs = split_list(&raw_keyexprs);
        if keyexprs.is_empty() {
            error!(
                "Zenoh {}: {}{}_KEYEXPRS is empty. Drop it.",
                label, prefix, id
            );
            continue;
        }

        let routing = match env.get_optional::<String>(&format!("{}{}_ROUTING", prefix, id)) {
            Some(raw) => match ZenohBucketRouting::from_str(&raw) {
                Ok(routing) => routing,
                Err(_) => {
                    error!(
                        "Zenoh {}: invalid routing '{}', expected 'static' or 'key-prefix'. Drop it.",
                        label, raw
                    );
                    continue;
                }
            },
            None => ZenohBucketRouting::Static,
        };

        let bucket =
            parse_optional_string(env.get_optional::<String>(&format!("{}{}_BUCKET", prefix, id)));
        if let Some(name) = &bucket {
            if !is_valid_bucket_name(name) {
                error!(
                    "Zenoh {}: invalid bucket name '{}', only letters, digits and [-,_] are allowed. Drop it.",
                    label, name
                );
                continue;
            }
        }

        let allowlist = env
            .get_optional::<String>(&format!("{}{}_BUCKET_ALLOWLIST", prefix, id))
            .map(|raw| split_list(&raw))
            .unwrap_or_default();
        for pattern in &allowlist {
            if pattern.contains('/') || pattern.contains("$*") {
                warn!(
                    "Zenoh {}: allowlist pattern '{}' looks like a Zenoh key expression. \
                     {}{}_BUCKET_ALLOWLIST takes plain globs matched against bucket names.",
                    label, pattern, prefix, id
                );
            }
        }

        let allow_bucket_creation = parse_bool(
            env.get_optional::<String>(&format!("{}{}_ALLOW_BUCKET_CREATION", prefix, id)),
            false,
        );

        let block = match routing {
            ZenohBucketRouting::Static => {
                let Some(bucket) = bucket else {
                    error!(
                        "Zenoh {}: static routing needs {}{}_BUCKET. Drop it.",
                        label, prefix, id
                    );
                    continue;
                };

                if !allowlist.is_empty() {
                    warn!(
                        "Zenoh {}: {}{}_BUCKET_ALLOWLIST is ignored in static routing",
                        label, prefix, id
                    );
                }
                if allow_bucket_creation {
                    warn!(
                        "Zenoh {}: {}{}_ALLOW_BUCKET_CREATION is ignored in static routing, \
                         bucket '{}' is created at startup",
                        label, prefix, id, bucket
                    );
                }

                ZenohBlock {
                    id,
                    keyexprs,
                    routing,
                    bucket: Some(bucket),
                    bucket_allowlist: Vec::new(),
                    allow_bucket_creation: false,
                }
            }
            ZenohBucketRouting::KeyPrefix => {
                if allowlist.is_empty() {
                    error!(
                        "Zenoh {}: key-prefix routing needs {}{}_BUCKET_ALLOWLIST to bound which \
                         buckets a key prefix can reach. Drop it.",
                        label, prefix, id
                    );
                    continue;
                }
                if bucket.is_none() {
                    debug!(
                        "Zenoh {}: no {}{}_BUCKET fallback, keys with a single chunk are rejected",
                        label, prefix, id
                    );
                }

                ZenohBlock {
                    id,
                    keyexprs,
                    routing,
                    bucket,
                    bucket_allowlist: allowlist,
                    allow_bucket_creation,
                }
            }
        };

        blocks.push(block);
    }

    warn_orphan_block_keys(env, prefix, kind, &discovered_ids);
    blocks.sort_by_key(|block| block_sort_key(&block.id));
    blocks
}

fn parse_legacy_blocks<EnvGetter: GetEnv>(
    env: &mut Env<EnvGetter>,
) -> (Vec<ZenohBlock>, Vec<ZenohQueryableBlock>) {
    let bucket = parse_optional_string(env.get_optional::<String>("RS_ZENOH_BUCKET"))
        .unwrap_or_else(|| DEFAULT_BUCKET.to_string());

    let legacy_block = |keyexprs: Vec<String>| ZenohBlock {
        id: LEGACY_BLOCK_ID.to_string(),
        keyexprs,
        routing: ZenohBucketRouting::Static,
        bucket: Some(bucket.clone()),
        bucket_allowlist: Vec::new(),
        allow_bucket_creation: false,
    };

    let subscribers = env
        .get_optional::<String>("RS_ZENOH_SUB_KEYEXPRS")
        .map(|raw| split_list(&raw))
        .filter(|keyexprs| !keyexprs.is_empty())
        .map(|keyexprs| vec![legacy_block(keyexprs)])
        .unwrap_or_default();

    let queryable_keyexprs = env
        .get_optional::<String>("RS_ZENOH_QUERY_KEYEXPRS")
        .map(|raw| split_list(&raw))
        .filter(|keyexprs| !keyexprs.is_empty());
    let queryables = match queryable_keyexprs {
        Some(keyexprs) => {
            let locality = parse_locality(
                env.get_optional::<String>("RS_ZENOH_QUERY_LOCALITY"),
                "queryable[legacy]",
            );
            vec![ZenohQueryableBlock {
                route: legacy_block(keyexprs),
                locality,
            }]
        }
        None => Vec::new(),
    };

    if !subscribers.is_empty() || !queryables.is_empty() {
        info!(
            "Zenoh: using the unindexed configuration as a single static block on bucket '{}'. \
             Consider migrating to RS_ZENOH_SUB_<ID>_* / RS_ZENOH_QUERY_<ID>_*",
            bucket
        );
    }

    (subscribers, queryables)
}

fn warn_orphan_block_keys<EnvGetter: GetEnv>(
    env: &Env<EnvGetter>,
    prefix: &str,
    kind: &str,
    discovered_ids: &[String],
) {
    for key in env.keys_with_prefix(prefix) {
        if let Some(id) = block_var_id(&key, prefix) {
            if !discovered_ids.iter().any(|known| known == id) {
                warn!(
                    "Zenoh: '{}' is ignored because {} block '{}' has no {}{}_KEYEXPRS",
                    key, kind, id, prefix, id
                );
            }
        }
    }
}

fn parse_optional_string(raw: Option<String>) -> Option<String> {
    raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn parse_locality(raw: Option<String>, label: &str) -> ZenohQueryableLocality {
    match raw {
        Some(value) => ZenohQueryableLocality::from_str(&value).unwrap_or_else(|_| {
            warn!(
                "Zenoh {}: invalid locality '{}', falling back to '{}'",
                label,
                value,
                ZenohQueryableLocality::default()
            );
            ZenohQueryableLocality::default()
        }),
        None => ZenohQueryableLocality::default(),
    }
}

fn split_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect()
}

// mirrors storage::engine::check_name_convention, which is not reachable from cfg
fn is_valid_bucket_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

// matched from the end so `_BUCKET` does not shadow `_BUCKET_ALLOWLIST`
fn block_var_id<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    BLOCK_SUFFIXES.iter().find_map(|suffix| {
        let id = rest.strip_suffix(suffix)?.strip_suffix('_')?;
        (!id.is_empty()).then_some(id)
    })
}

fn is_indexed_block_key(key: &str) -> bool {
    block_var_id(key, SUB_PREFIX).is_some() || block_var_id(key, QUERY_PREFIX).is_some()
}

fn block_sort_key(id: &str) -> (u8, u64, String) {
    match id.parse::<u64>() {
        Ok(index) => (0, index, String::new()),
        Err(_) => (1, 0, id.to_string()),
    }
}

fn join_keys(keys: &[&String]) -> String {
    keys.iter()
        .map(|key| key.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_blocks<T: Display>(blocks: &[T]) -> String {
    if blocks.is_empty() {
        return "<disabled>".to_string();
    }

    blocks
        .iter()
        .map(|block| format!("[{}]", block))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::TestEnvGetter;
    use rstest::rstest;

    fn parse(vars: &[(&str, &str)]) -> ZenohApiConfig {
        let getter = TestEnvGetter::new(vars);
        CfgParser::<TestEnvGetter>::parse_zenoh_api_config(&mut Env::new(getter))
    }

    #[rstest]
    fn parses_single_indexed_subscriber_block() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
        ]);

        assert_eq!(cfg.subscribers.len(), 1);
        let block = &cfg.subscribers[0];
        assert_eq!(block.id, "0");
        assert_eq!(block.keyexprs, vec!["raw/**".to_string()]);
        assert_eq!(block.routing, ZenohBucketRouting::Static);
        assert_eq!(block.bucket, Some("raw_data".to_string()));
        assert!(block.bucket_allowlist.is_empty());
        assert!(!block.allow_bucket_creation);
        assert!(cfg.queryables.is_empty());
    }

    #[rstest]
    fn parses_multiple_blocks_in_natural_id_order() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "a/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "bucket_a"),
            ("RS_ZENOH_SUB_1_KEYEXPRS", "b/**"),
            ("RS_ZENOH_SUB_1_BUCKET", "bucket_b"),
            ("RS_ZENOH_SUB_2_KEYEXPRS", "c/**"),
            ("RS_ZENOH_SUB_2_BUCKET", "bucket_c"),
            ("RS_ZENOH_SUB_10_KEYEXPRS", "d/**"),
            ("RS_ZENOH_SUB_10_BUCKET", "bucket_d"),
        ]);

        let ids: Vec<&str> = cfg.subscribers.iter().map(|b| b.id.as_str()).collect();
        assert_eq!(ids, vec!["0", "1", "2", "10"]);
        assert_eq!(cfg.subscribers[3].bucket, Some("bucket_d".to_string()));
    }

    #[rstest]
    fn parses_several_keyexprs_for_one_bucket() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_2_KEYEXPRS", "events/**, alarms/** ,,"),
            ("RS_ZENOH_SUB_2_BUCKET", "events_data"),
        ]);

        assert_eq!(cfg.subscribers.len(), 1);
        assert_eq!(
            cfg.subscribers[0].keyexprs,
            vec!["events/**".to_string(), "alarms/**".to_string()]
        );
    }

    #[rstest]
    fn parses_key_prefix_block_with_allowlist() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_3_KEYEXPRS", "site_$*/**,cell_$*/**"),
            ("RS_ZENOH_SUB_3_ROUTING", "key-prefix"),
            ("RS_ZENOH_SUB_3_BUCKET_ALLOWLIST", "site_*,cell_*"),
            ("RS_ZENOH_SUB_3_ALLOW_BUCKET_CREATION", "true"),
        ]);

        let block = &cfg.subscribers[0];
        assert_eq!(block.routing, ZenohBucketRouting::KeyPrefix);
        assert_eq!(block.bucket, None);
        assert_eq!(
            block.bucket_allowlist,
            vec!["site_*".to_string(), "cell_*".to_string()]
        );
        assert!(block.allow_bucket_creation);
    }

    #[rstest]
    fn parses_key_prefix_block_with_fallback_bucket() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "site_$*/**"),
            ("RS_ZENOH_SUB_0_ROUTING", "key_prefix"),
            ("RS_ZENOH_SUB_0_BUCKET_ALLOWLIST", "site_*"),
            ("RS_ZENOH_SUB_0_BUCKET", "zenoh_misc"),
        ]);

        let block = &cfg.subscribers[0];
        assert_eq!(block.routing, ZenohBucketRouting::KeyPrefix);
        assert_eq!(block.bucket, Some("zenoh_misc".to_string()));
        assert!(!block.allow_bucket_creation);
    }

    #[rstest]
    fn parses_queryable_locality_per_block() {
        let cfg = parse(&[
            ("RS_ZENOH_QUERY_0_KEYEXPRS", "a/**"),
            ("RS_ZENOH_QUERY_0_BUCKET", "bucket_a"),
            ("RS_ZENOH_QUERY_0_LOCALITY", "Remote"),
            ("RS_ZENOH_QUERY_1_KEYEXPRS", "b/**"),
            ("RS_ZENOH_QUERY_1_BUCKET", "bucket_b"),
        ]);

        assert_eq!(cfg.queryables.len(), 2);
        assert_eq!(cfg.queryables[0].locality, ZenohQueryableLocality::Remote);
        assert_eq!(cfg.queryables[1].locality, ZenohQueryableLocality::Any);
        assert!(cfg.subscribers.is_empty());
    }

    #[rstest]
    fn parses_globals_alongside_indexed_blocks() {
        let cfg = parse(&[
            ("RS_ZENOH_ENABLED", "yes"),
            ("RS_ZENOH_CONFIG", "mode=client;peer=localhost:7447"),
            ("RS_ZENOH_TLS_ROOT_CA", "root-ca"),
            ("RS_ZENOH_AUTH_DICTIONARY", "user:pass"),
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
        ]);

        assert!(cfg.enabled);
        assert_eq!(
            cfg.config_inline,
            Some("mode=client;peer=localhost:7447".to_string())
        );
        assert_eq!(cfg.config_path, None);
        assert_eq!(cfg.tls_root_ca_cert, Some("root-ca".to_string()));
        assert_eq!(cfg.auth_dictionary, Some("user:pass".to_string()));
        assert_eq!(cfg.subscribers.len(), 1);
    }

    #[rstest]
    fn parses_config_path() {
        let cfg = parse(&[
            ("RS_ZENOH_ENABLED", "true"),
            ("RS_ZENOH_CONFIG_PATH", "/etc/reductstore/zenoh.json5"),
        ]);

        assert!(cfg.enabled);
        assert_eq!(cfg.config_inline, None);
        assert_eq!(
            cfg.config_path,
            Some("/etc/reductstore/zenoh.json5".to_string())
        );
    }

    #[rstest]
    fn parses_reviewed_example_configuration() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
            ("RS_ZENOH_SUB_1_KEYEXPRS", "derived/**"),
            ("RS_ZENOH_SUB_1_BUCKET", "derived_data"),
            ("RS_ZENOH_SUB_2_KEYEXPRS", "events/**,alarms/**"),
            ("RS_ZENOH_SUB_2_BUCKET", "events_data"),
            ("RS_ZENOH_SUB_3_KEYEXPRS", "site_$*/**,cell_$*/**"),
            ("RS_ZENOH_SUB_3_ROUTING", "key-prefix"),
            ("RS_ZENOH_SUB_3_BUCKET_ALLOWLIST", "site_*,cell_*"),
            ("RS_ZENOH_SUB_3_ALLOW_BUCKET_CREATION", "true"),
            ("RS_ZENOH_QUERY_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_QUERY_0_BUCKET", "raw_data"),
            ("RS_ZENOH_QUERY_1_KEYEXPRS", "site_$*/**,cell_$*/**"),
            ("RS_ZENOH_QUERY_1_ROUTING", "key-prefix"),
            ("RS_ZENOH_QUERY_1_BUCKET_ALLOWLIST", "site_*,cell_*"),
        ]);

        assert_eq!(cfg.subscribers.len(), 4);
        assert_eq!(cfg.queryables.len(), 2);

        assert_eq!(cfg.subscribers[2].keyexprs.len(), 2);
        assert_eq!(cfg.subscribers[2].bucket, Some("events_data".to_string()));

        assert!(cfg.subscribers[3].is_key_prefix());
        assert!(cfg.subscribers[3].allow_bucket_creation);

        assert!(cfg.queryables[1].route.is_key_prefix());
        assert!(!cfg.queryables[1].route.allow_bucket_creation);
        assert_eq!(
            cfg.queryables[1].route.bucket_allowlist,
            vec!["site_*".to_string(), "cell_*".to_string()]
        );
    }

    #[rstest]
    fn drops_static_block_without_bucket() {
        let cfg = parse(&[("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**")]);
        assert!(cfg.subscribers.is_empty());
    }

    #[rstest]
    fn drops_block_with_empty_keyexprs() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", " , , "),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
        ]);
        assert!(cfg.subscribers.is_empty());
    }

    #[rstest]
    fn drops_block_with_invalid_routing() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
            ("RS_ZENOH_SUB_0_ROUTING", "dynamic"),
        ]);
        assert!(cfg.subscribers.is_empty());
    }

    #[rstest]
    #[case("raw/data")]
    #[case("raw.data")]
    #[case("$system")]
    fn drops_block_with_invalid_bucket_name(#[case] name: &str) {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", name),
        ]);
        assert!(cfg.subscribers.is_empty());
    }

    #[rstest]
    fn drops_key_prefix_block_without_allowlist() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "**"),
            ("RS_ZENOH_SUB_0_ROUTING", "key-prefix"),
        ]);
        assert!(cfg.subscribers.is_empty());
    }

    #[rstest]
    fn drops_only_the_invalid_block() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "a/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "bucket_a"),
            ("RS_ZENOH_SUB_1_KEYEXPRS", "b/**"),
            ("RS_ZENOH_SUB_2_KEYEXPRS", "c/**"),
            ("RS_ZENOH_SUB_2_BUCKET", "bucket_c"),
        ]);

        let ids: Vec<&str> = cfg.subscribers.iter().map(|b| b.id.as_str()).collect();
        assert_eq!(ids, vec!["0", "2"]);
    }

    #[rstest]
    fn ignores_allowlist_and_creation_on_static_block() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
            ("RS_ZENOH_SUB_0_BUCKET_ALLOWLIST", "site_*"),
            ("RS_ZENOH_SUB_0_ALLOW_BUCKET_CREATION", "true"),
        ]);

        let block = &cfg.subscribers[0];
        assert!(block.bucket_allowlist.is_empty());
        assert!(!block.allow_bucket_creation);
    }

    #[rstest]
    fn invalid_locality_falls_back_to_any() {
        let cfg = parse(&[
            ("RS_ZENOH_QUERY_0_KEYEXPRS", "a/**"),
            ("RS_ZENOH_QUERY_0_BUCKET", "bucket_a"),
            ("RS_ZENOH_QUERY_0_LOCALITY", "nowhere"),
        ]);

        assert_eq!(cfg.queryables.len(), 1);
        assert_eq!(cfg.queryables[0].locality, ZenohQueryableLocality::Any);
    }

    #[rstest]
    fn invalid_allow_bucket_creation_falls_back_to_false() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_KEYEXPRS", "site_$*/**"),
            ("RS_ZENOH_SUB_0_ROUTING", "key-prefix"),
            ("RS_ZENOH_SUB_0_BUCKET_ALLOWLIST", "site_*"),
            ("RS_ZENOH_SUB_0_ALLOW_BUCKET_CREATION", "maybe"),
        ]);

        assert!(!cfg.subscribers[0].allow_bucket_creation);
    }

    #[rstest]
    fn synthesizes_legacy_block_from_unindexed_vars() {
        let cfg = parse(&[
            ("RS_ZENOH_BUCKET", "telemetry"),
            ("RS_ZENOH_SUB_KEYEXPRS", "**"),
            ("RS_ZENOH_QUERY_KEYEXPRS", "factory/**"),
            ("RS_ZENOH_QUERY_LOCALITY", "Remote"),
        ]);

        assert_eq!(cfg.subscribers.len(), 1);
        let sub = &cfg.subscribers[0];
        assert_eq!(sub.id, LEGACY_BLOCK_ID);
        assert_eq!(sub.routing, ZenohBucketRouting::Static);
        assert_eq!(sub.bucket, Some("telemetry".to_string()));
        assert_eq!(sub.keyexprs, vec!["**".to_string()]);

        assert_eq!(cfg.queryables.len(), 1);
        assert_eq!(
            cfg.queryables[0].route.keyexprs,
            vec!["factory/**".to_string()]
        );
        assert_eq!(cfg.queryables[0].locality, ZenohQueryableLocality::Remote);
    }

    #[rstest]
    fn legacy_defaults_to_zenoh_bucket() {
        let cfg = parse(&[("RS_ZENOH_SUB_KEYEXPRS", "**")]);
        assert_eq!(cfg.subscribers[0].bucket, Some(DEFAULT_BUCKET.to_string()));
    }

    #[rstest]
    fn legacy_empty_bucket_falls_back_to_default() {
        let cfg = parse(&[("RS_ZENOH_BUCKET", "   "), ("RS_ZENOH_SUB_KEYEXPRS", "**")]);
        assert_eq!(cfg.subscribers[0].bucket, Some(DEFAULT_BUCKET.to_string()));
    }

    #[rstest]
    fn legacy_sub_only_leaves_queryables_empty() {
        let cfg = parse(&[("RS_ZENOH_SUB_KEYEXPRS", "**")]);
        assert_eq!(cfg.subscribers.len(), 1);
        assert!(cfg.queryables.is_empty());
    }

    #[rstest]
    fn legacy_query_only_leaves_subscribers_empty() {
        let cfg = parse(&[("RS_ZENOH_QUERY_KEYEXPRS", "**")]);
        assert!(cfg.subscribers.is_empty());
        assert_eq!(cfg.queryables.len(), 1);
    }

    #[rstest]
    fn legacy_keyexprs_is_not_seen_as_indexed() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_KEYEXPRS", "**"),
            ("RS_ZENOH_QUERY_KEYEXPRS", "**"),
            ("RS_ZENOH_QUERY_LOCALITY", "Any"),
        ]);

        assert_eq!(cfg.subscribers[0].id, LEGACY_BLOCK_ID);
        assert_eq!(cfg.queryables[0].route.id, LEGACY_BLOCK_ID);
    }

    #[rstest]
    fn indexed_vars_win_over_legacy_vars() {
        let cfg = parse(&[
            ("RS_ZENOH_BUCKET", "legacy_bucket"),
            ("RS_ZENOH_SUB_KEYEXPRS", "**"),
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
        ]);

        assert_eq!(cfg.subscribers.len(), 1);
        assert_eq!(cfg.subscribers[0].id, "0");
        assert_eq!(cfg.subscribers[0].bucket, Some("raw_data".to_string()));
    }

    #[rstest]
    fn orphan_indexed_var_disables_legacy_and_yields_no_blocks() {
        let cfg = parse(&[
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
            ("RS_ZENOH_BUCKET", "legacy_bucket"),
            ("RS_ZENOH_SUB_KEYEXPRS", "**"),
            ("RS_ZENOH_QUERY_KEYEXPRS", "**"),
        ]);

        assert!(cfg.subscribers.is_empty());
        assert!(cfg.queryables.is_empty());
    }

    #[rstest]
    fn no_zenoh_vars_yields_no_blocks() {
        let cfg = parse(&[]);

        assert!(!cfg.enabled);
        assert_eq!(cfg.config_inline, None);
        assert_eq!(cfg.config_path, None);
        assert!(cfg.subscribers.is_empty());
        assert!(cfg.queryables.is_empty());
    }

    #[rstest]
    fn parses_invalid_enabled_falls_back_to_default() {
        let cfg = parse(&[("RS_ZENOH_ENABLED", "maybe")]);
        assert!(!cfg.enabled);
    }

    #[rstest]
    #[case("", vec![])]
    #[case(",", vec![])]
    #[case(" a , ,b ", vec!["a", "b"])]
    #[case("single", vec!["single"])]
    fn split_list_trims_and_drops_empties(#[case] raw: &str, #[case] expected: Vec<&str>) {
        assert_eq!(split_list(raw), expected);
    }

    #[rstest]
    #[case("RS_ZENOH_SUB_0_KEYEXPRS", Some("0"))]
    #[case("RS_ZENOH_SUB_0_BUCKET", Some("0"))]
    #[case("RS_ZENOH_SUB_0_BUCKET_ALLOWLIST", Some("0"))]
    #[case("RS_ZENOH_SUB_0_ALLOW_BUCKET_CREATION", Some("0"))]
    #[case("RS_ZENOH_SUB_a_b_ROUTING", Some("a_b"))]
    #[case("RS_ZENOH_SUB_KEYEXPRS", None)]
    #[case("RS_ZENOH_SUB__BUCKET", None)]
    #[case("RS_ZENOH_BUCKET", None)]
    fn block_var_id_extracts_ids(#[case] key: &str, #[case] expected: Option<&str>) {
        assert_eq!(block_var_id(key, SUB_PREFIX), expected);
    }

    #[rstest]
    #[case("RS_ZENOH_QUERY_LOCALITY", false)]
    #[case("RS_ZENOH_QUERY_0_LOCALITY", true)]
    #[case("RS_ZENOH_SUB_KEYEXPRS", false)]
    #[case("RS_ZENOH_ENABLED", false)]
    fn detects_indexed_block_keys(#[case] key: &str, #[case] expected: bool) {
        assert_eq!(is_indexed_block_key(key), expected);
    }

    #[rstest]
    #[case("raw_data", true)]
    #[case("a-b", true)]
    #[case("A1", true)]
    #[case("", false)]
    #[case("raw/data", false)]
    #[case("raw.data", false)]
    #[case("site_*", false)]
    #[case("$system", false)]
    fn is_valid_bucket_name_matches_storage_convention(#[case] name: &str, #[case] expected: bool) {
        assert_eq!(is_valid_bucket_name(name), expected);
    }

    #[rstest]
    fn block_sort_key_orders_numeric_ids_first() {
        let mut ids = vec!["b", "10", "2", "a"];
        ids.sort_by_key(|id| block_sort_key(id));
        assert_eq!(ids, vec!["2", "10", "a", "b"]);
    }

    #[rstest]
    #[case("static", ZenohBucketRouting::Static)]
    #[case("key-prefix", ZenohBucketRouting::KeyPrefix)]
    #[case("Key_Prefix", ZenohBucketRouting::KeyPrefix)]
    #[case("KEYPREFIX", ZenohBucketRouting::KeyPrefix)]
    fn parses_bucket_routing(#[case] raw: &str, #[case] expected: ZenohBucketRouting) {
        assert_eq!(ZenohBucketRouting::from_str(raw), Ok(expected));
    }

    #[rstest]
    fn rejects_invalid_bucket_routing() {
        assert!(ZenohBucketRouting::from_str("dynamic").is_err());
    }

    #[rstest]
    fn test_display() {
        let cfg = parse(&[
            ("RS_ZENOH_ENABLED", "true"),
            ("RS_ZENOH_CONFIG", "mode=client"),
            ("RS_ZENOH_CONFIG_PATH", "/etc/zenoh.json5"),
            ("RS_ZENOH_TLS_CONNECT_CERT", "-----BEGIN CERTIFICATE-----"),
            ("RS_ZENOH_SUB_0_KEYEXPRS", "raw/**"),
            ("RS_ZENOH_SUB_0_BUCKET", "raw_data"),
            ("RS_ZENOH_SUB_1_KEYEXPRS", "site_$*/**"),
            ("RS_ZENOH_SUB_1_ROUTING", "key-prefix"),
            ("RS_ZENOH_SUB_1_BUCKET_ALLOWLIST", "site_*,cell_*"),
            ("RS_ZENOH_SUB_1_ALLOW_BUCKET_CREATION", "true"),
        ]);

        let display = format!("{cfg}");
        assert!(display.contains("enabled=true"));
        assert!(display.contains("config=mode=client"));
        assert!(display.contains("config_path=/etc/zenoh.json5"));
        assert!(display.contains("tls=true"));
        assert!(display.contains("[0: keyexprs=raw/**, routing=static, bucket=raw_data]"));
        assert!(display.contains(
            "[1: keyexprs=site_$*/**, routing=key-prefix, bucket=<none>, \
             allowlist=site_*,cell_*, allow_bucket_creation=true]"
        ));
        assert!(display.contains("queryables=<disabled>"));
    }

    #[rstest]
    fn test_display_defaults() {
        let display = format!("{}", ZenohApiConfig::default());
        assert!(display.contains("enabled=false"));
        assert!(display.contains("config=<none>"));
        assert!(display.contains("config_path=<none>"));
        assert!(display.contains("tls=false"));
        assert!(display.contains("auth=false"));
        assert!(display.contains("subscribers=<disabled>"));
        assert!(display.contains("queryables=<disabled>"));
    }
}
