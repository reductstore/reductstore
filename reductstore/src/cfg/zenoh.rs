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
}

impl Default for ZenohApiConfig {
    fn default() -> Self {
        ZenohApiConfig {
            enabled: false,
            key_prefix: "reduct".to_string(),
            listen_endpoints: Vec::new(),
            connect_endpoints: Vec::new(),
            mode: None,
            disable_multicast_scouting: false,
        }
    }
}

impl Display for ZenohApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, prefix={}, listen={:?}, connect={:?}, mode={:?}, disable_multicast={}",
            self.enabled,
            self.key_prefix,
            self.listen_endpoints,
            self.connect_endpoints,
            self.mode,
            self.disable_multicast_scouting
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
                .unwrap_or_else(|| "reduct".to_string()),
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

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(cfg.enabled);
        assert_eq!(cfg.key_prefix, "telemetry");
        assert_eq!(cfg.listen_endpoints.len(), 2);
        assert_eq!(cfg.connect_endpoints, vec!["tcp/10.0.0.1:7447".to_string()]);
        assert_eq!(cfg.mode, Some("peer".to_string()));
        assert!(cfg.disable_multicast_scouting);
    }

    #[rstest]
    fn parses_defaults() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(!cfg.enabled);
        assert_eq!(cfg.key_prefix, "reduct");
        assert!(cfg.listen_endpoints.is_empty());
        assert!(cfg.connect_endpoints.is_empty());
        assert_eq!(cfg.mode, None);
        assert!(!cfg.disable_multicast_scouting);
    }
}
