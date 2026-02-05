// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use std::fmt::{Display, Formatter};

/// Configuration for the optional Zenoh API bridge.
#[derive(Clone, Debug, PartialEq)]
pub struct ZenohApiConfig {
    /// Enables the Zenoh API runtime.
    pub enabled: bool,
    /// Prefix for all Zenoh key expressions served by ReductStore.
    pub key_prefix: String,
    /// Endpoints this node should listen on.
    pub listen_endpoints: Vec<String>,
    /// Endpoints this node should connect to as a client/peer.
    pub connect_endpoints: Vec<String>,
    /// Optional mode override (`peer`, `client`, `router`).
    pub mode: Option<String>,
    /// True to disable multicast scouting for discovery.
    pub disable_multicast_scouting: bool,
    /// Key expression patterns to subscribe to for data ingestion.
    /// Each pattern maps to bucket/entry based on the key structure: `{prefix}/{bucket}/{entry}/**`
    pub subscribe_patterns: Vec<String>,
    /// Enable advanced subscriber features: history recovery and sample miss detection.
    pub enable_recovery: bool,
    /// Enable queryable endpoints to allow Zenoh queries to read data from ReductStore.
    pub enable_queryable: bool,
}

const DEFAULT_KEY_PREFIX: &str = "reduct";

impl Default for ZenohApiConfig {
    fn default() -> Self {
        ZenohApiConfig {
            enabled: false,
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            listen_endpoints: Vec::new(),
            connect_endpoints: Vec::new(),
            mode: None,
            disable_multicast_scouting: false,
            subscribe_patterns: Vec::new(),
            enable_recovery: false,
            enable_queryable: true,
        }
    }
}

impl Display for ZenohApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, prefix={}, listen={:?}, connect={:?}, mode={:?}, disable_multicast={}, subscribe={:?}, recovery={}, queryable={}",
            self.enabled,
            self.key_prefix,
            self.listen_endpoints,
            self.connect_endpoints,
            self.mode,
            self.disable_multicast_scouting,
            self.subscribe_patterns,
            self.enable_recovery,
            self.enable_queryable
        )
    }
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub(super) fn parse_zenoh_api_config(env: &mut Env<EnvGetter>) -> ZenohApiConfig {
        ZenohApiConfig {
            enabled: parse_bool(env.get_optional::<String>("RS_ZENOH_ENABLED"), false),
            key_prefix: env
                .get_optional("RS_ZENOH_KEY_PREFIX")
                .filter(|value: &String| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_KEY_PREFIX.to_string()),
            listen_endpoints: parse_endpoints(env.get_optional::<String>("RS_ZENOH_LISTEN")),
            connect_endpoints: parse_endpoints(env.get_optional::<String>("RS_ZENOH_CONNECT")),
            mode: env
                .get_optional::<String>("RS_ZENOH_MODE")
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_lowercase())
                    }
                }),
            disable_multicast_scouting: parse_bool(
                env.get_optional::<String>("RS_ZENOH_DISABLE_MULTICAST"),
                false,
            ),
            subscribe_patterns: parse_endpoints(env.get_optional::<String>("RS_ZENOH_SUBSCRIBE")),
            enable_recovery: parse_bool(
                env.get_optional::<String>("RS_ZENOH_ENABLE_RECOVERY"),
                false,
            ),
            enable_queryable: parse_bool(
                env.get_optional::<String>("RS_ZENOH_ENABLE_QUERYABLE"),
                true,
            ),
        }
    }
}

fn parse_bool(raw: Option<String>, default: bool) -> bool {
    raw.map(|value| match value.trim().to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    })
    .unwrap_or(default)
}

fn parse_endpoints(raw: Option<String>) -> Vec<String> {
    raw.map(|value| {
        value
            .split(|ch| ch == ',' || ch == ';' || ch == '\n')
            .map(|segment| segment.trim().to_string())
            .filter(|segment| !segment.is_empty())
            .collect()
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::rstest;
    use std::env::VarError;

    #[rstest]
    fn parses_custom_values() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_ENABLED"))
            .times(1)
            .return_const(Ok("yes".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_KEY_PREFIX"))
            .times(1)
            .return_const(Ok("telemetry".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_LISTEN"))
            .times(1)
            .return_const(Ok("tcp/0.0.0.0:7447;udp/224.0.0.1:7447".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_CONNECT"))
            .times(1)
            .return_const(Ok("tcp/10.0.0.1:7447".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_MODE"))
            .times(1)
            .return_const(Ok("Peer".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_DISABLE_MULTICAST"))
            .times(1)
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_SUBSCRIBE"))
            .times(1)
            .return_const(Ok("sensors/**,telemetry/imu/**".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_ENABLE_RECOVERY"))
            .times(1)
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_ENABLE_QUERYABLE"))
            .times(1)
            .return_const(Ok("false".to_string()));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(cfg.enabled);
        assert_eq!(cfg.key_prefix, "telemetry");
        assert_eq!(cfg.listen_endpoints.len(), 2);
        assert_eq!(cfg.connect_endpoints, vec!["tcp/10.0.0.1:7447".to_string()]);
        assert_eq!(cfg.mode, Some("peer".to_string()));
        assert!(cfg.disable_multicast_scouting);
        assert_eq!(
            cfg.subscribe_patterns,
            vec!["sensors/**".to_string(), "telemetry/imu/**".to_string()]
        );
        assert!(cfg.enable_recovery);
        assert!(!cfg.enable_queryable);
    }

    #[rstest]
    fn parses_defaults() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(!cfg.enabled);
        assert_eq!(cfg.key_prefix, DEFAULT_KEY_PREFIX.to_string());
        assert!(cfg.listen_endpoints.is_empty());
        assert!(cfg.connect_endpoints.is_empty());
        assert_eq!(cfg.mode, None);
        assert!(!cfg.disable_multicast_scouting);
        assert!(cfg.subscribe_patterns.is_empty());
        assert!(!cfg.enable_recovery);
        assert!(cfg.enable_queryable);
    }
}
