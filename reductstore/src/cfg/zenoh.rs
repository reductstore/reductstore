// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use std::fmt::{Display, Formatter};

const DEFAULT_BUCKET: &str = "zenoh";

/// Configuration for the minimal Zenoh API integration (single-bucket mode).
///
/// # Supported Environment Variables
///
/// - `RS_ZENOH_ENABLED`: Enable/disable the Zenoh integration (default: false)
/// - `RS_ZENOH_CONFIG`: Inline Zenoh config string (e.g., "mode=client;peer=localhost:7447")
/// - `RS_ZENOH_CONFIG_PATH`: Path to a Zenoh JSON5 config file
/// - `RS_ZENOH_BUCKET`: The single bucket for all Zenoh data (default: "zenoh")
/// - `RS_ZENOH_SUB_KEYEXPRS`: Key expression for subscriber (write path), e.g., "**"
/// - `RS_ZENOH_QUERY_KEYEXPRS`: Key expression for queryable (read path), e.g., "**"
///
/// If both `RS_ZENOH_CONFIG` and `RS_ZENOH_CONFIG_PATH` are set, inline config takes precedence.
#[derive(Clone, Debug, PartialEq)]
pub struct ZenohApiConfig {
    /// Enables the Zenoh API runtime.
    pub enabled: bool,
    /// Inline Zenoh configuration string (e.g., "mode=client;peer=localhost:7447").
    /// Takes precedence over `config_path` if both are set.
    pub config_inline: Option<String>,
    /// Path to a Zenoh JSON5 configuration file.
    pub config_path: Option<String>,
    /// The single ReductStore bucket used for all Zenoh data.
    pub bucket: String,
    /// Key expression for the Zenoh subscriber (write path).
    /// If unset, the write path is disabled.
    pub sub_keyexprs: Option<String>,
    /// Key expression for the Zenoh queryable (read path).
    /// If unset, the read path is disabled.
    pub query_keyexprs: Option<String>,
}

impl Default for ZenohApiConfig {
    fn default() -> Self {
        ZenohApiConfig {
            enabled: false,
            config_inline: None,
            config_path: None,
            bucket: DEFAULT_BUCKET.to_string(),
            sub_keyexprs: None,
            query_keyexprs: None,
        }
    }
}

impl Display for ZenohApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, bucket={}, config={}, config_path={}, sub_keyexprs={}, query_keyexprs={}",
            self.enabled,
            self.bucket,
            self.config_inline.as_deref().unwrap_or("<none>"),
            self.config_path.as_deref().unwrap_or("<none>"),
            self.sub_keyexprs.as_deref().unwrap_or("<disabled>"),
            self.query_keyexprs.as_deref().unwrap_or("<disabled>")
        )
    }
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub(super) fn parse_zenoh_api_config(env: &mut Env<EnvGetter>) -> ZenohApiConfig {
        ZenohApiConfig {
            enabled: parse_bool(env.get_optional::<String>("RS_ZENOH_ENABLED"), false),
            config_inline: parse_optional_string(env.get_optional::<String>("RS_ZENOH_CONFIG")),
            config_path: parse_optional_string(env.get_optional::<String>("RS_ZENOH_CONFIG_PATH")),
            bucket: env
                .get_optional("RS_ZENOH_BUCKET")
                .filter(|value: &String| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_BUCKET.to_string()),
            sub_keyexprs: parse_optional_string(
                env.get_optional::<String>("RS_ZENOH_SUB_KEYEXPRS"),
            ),
            query_keyexprs: parse_optional_string(
                env.get_optional::<String>("RS_ZENOH_QUERY_KEYEXPRS"),
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

fn parse_optional_string(raw: Option<String>) -> Option<String> {
    raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
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
            .with(eq("RS_ZENOH_CONFIG"))
            .times(1)
            .return_const(Ok("mode=client;peer=localhost:7447".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_CONFIG_PATH"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_BUCKET"))
            .times(1)
            .return_const(Ok("telemetry".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_SUB_KEYEXPRS"))
            .times(1)
            .return_const(Ok("**".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_QUERY_KEYEXPRS"))
            .times(1)
            .return_const(Ok("factory/**".to_string()));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(cfg.enabled);
        assert_eq!(
            cfg.config_inline,
            Some("mode=client;peer=localhost:7447".to_string())
        );
        assert_eq!(cfg.config_path, None);
        assert_eq!(cfg.bucket, "telemetry");
        assert_eq!(cfg.sub_keyexprs, Some("**".to_string()));
        assert_eq!(cfg.query_keyexprs, Some("factory/**".to_string()));
    }

    #[rstest]
    fn parses_config_path() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_ENABLED"))
            .times(1)
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_CONFIG"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_CONFIG_PATH"))
            .times(1)
            .return_const(Ok("/etc/reductstore/zenoh.json5".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_BUCKET"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_SUB_KEYEXPRS"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_QUERY_KEYEXPRS"))
            .times(1)
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(cfg.enabled);
        assert_eq!(cfg.config_inline, None);
        assert_eq!(
            cfg.config_path,
            Some("/etc/reductstore/zenoh.json5".to_string())
        );
        assert_eq!(cfg.bucket, DEFAULT_BUCKET);
        assert_eq!(cfg.sub_keyexprs, None);
        assert_eq!(cfg.query_keyexprs, None);
    }

    #[rstest]
    fn parses_defaults() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(!cfg.enabled);
        assert_eq!(cfg.config_inline, None);
        assert_eq!(cfg.config_path, None);
        assert_eq!(cfg.bucket, DEFAULT_BUCKET);
        assert_eq!(cfg.sub_keyexprs, None);
        assert_eq!(cfg.query_keyexprs, None);
    }
}
