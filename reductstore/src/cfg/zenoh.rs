// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{parse_bool, CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

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
/// - `RS_ZENOH_QUERY_LOCALITY`: Allowed origin for query replies. One of `SessionLocal`, `Remote`, or `Any` (default)
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
    /// Allowed locality for the Zenoh queryable responses.
    pub query_locality: ZenohQueryableLocality,
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

impl Default for ZenohApiConfig {
    fn default() -> Self {
        ZenohApiConfig {
            enabled: false,
            config_inline: None,
            config_path: None,
            bucket: DEFAULT_BUCKET.to_string(),
            sub_keyexprs: None,
            query_keyexprs: None,
            query_locality: ZenohQueryableLocality::default(),
            tls_root_ca_cert: None,
            tls_connect_cert: None,
            tls_connect_key: None,
            auth_dictionary: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZenohQueryableLocality {
    SessionLocal,
    Remote,
    Any,
}

impl Default for ZenohQueryableLocality {
    fn default() -> Self {
        ZenohQueryableLocality::Any
    }
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

impl Display for ZenohApiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, bucket={}, config={}, config_path={}, sub_keyexprs={}, query_keyexprs={}, query_locality={}, tls={}, auth={}",
            self.enabled,
            self.bucket,
            self.config_inline.as_deref().unwrap_or("<none>"),
            self.config_path.as_deref().unwrap_or("<none>"),
            self.sub_keyexprs.as_deref().unwrap_or("<disabled>"),
            self.query_keyexprs.as_deref().unwrap_or("<disabled>"),
            self.query_locality,
            self.tls_root_ca_cert.is_some() || self.tls_connect_cert.is_some(),
            self.auth_dictionary.is_some()
        )
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
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
            query_locality: parse_query_locality(
                env.get_optional::<String>("RS_ZENOH_QUERY_LOCALITY"),
            ),
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
        }
    }
}

fn parse_optional_string(raw: Option<String>) -> Option<String> {
    raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn parse_query_locality(raw: Option<String>) -> ZenohQueryableLocality {
    raw.and_then(|value| ZenohQueryableLocality::from_str(&value).ok())
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
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_QUERY_LOCALITY"))
            .times(1)
            .return_const(Ok("Remote".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_ROOT_CA"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_CONNECT_CERT"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_CONNECT_KEY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_AUTH_DICTIONARY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));

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
        assert_eq!(cfg.query_locality, ZenohQueryableLocality::Remote);
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
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_QUERY_LOCALITY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_ROOT_CA"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_CONNECT_CERT"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_CONNECT_KEY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_AUTH_DICTIONARY"))
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
        assert_eq!(cfg.query_locality, ZenohQueryableLocality::default());
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
        assert_eq!(cfg.query_locality, ZenohQueryableLocality::default());
    }

    #[rstest]
    fn parses_invalid_enabled_falls_back_to_default() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_ENABLED"))
            .times(1)
            .return_const(Ok("maybe".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert!(!cfg.enabled);
    }

    #[rstest]
    fn parses_empty_bucket_falls_back_to_default() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_ENABLED"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_CONFIG"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_CONFIG_PATH"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_BUCKET"))
            .times(1)
            .return_const(Ok("   ".to_string())); // whitespace-only
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
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_QUERY_LOCALITY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_ROOT_CA"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_CONNECT_CERT"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_TLS_CONNECT_KEY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_ZENOH_AUTH_DICTIONARY"))
            .times(1)
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_zenoh_api_config(&mut Env::new(env_getter));
        assert_eq!(cfg.bucket, DEFAULT_BUCKET);
    }

    #[rstest]
    fn test_display() {
        let cfg = ZenohApiConfig {
            enabled: true,
            config_inline: Some("mode=client".to_string()),
            config_path: Some("/etc/zenoh.json5".to_string()),
            bucket: "sensor-data".to_string(),
            sub_keyexprs: Some("**".to_string()),
            query_keyexprs: None,
            query_locality: ZenohQueryableLocality::Remote,
            tls_root_ca_cert: None,
            tls_connect_cert: Some("-----BEGIN CERTIFICATE-----".to_string()),
            tls_connect_key: None,
            auth_dictionary: None,
        };
        let display = format!("{cfg}");
        assert!(display.contains("enabled=true"));
        assert!(display.contains("bucket=sensor-data"));
        assert!(display.contains("config=mode=client"));
        assert!(display.contains("config_path=/etc/zenoh.json5"));
        assert!(display.contains("sub_keyexprs=**"));
        assert!(display.contains("query_keyexprs=<disabled>"));
        assert!(display.contains("query_locality=Remote"));
        assert!(display.contains("tls=true"));
    }

    #[rstest]
    fn test_display_defaults() {
        let cfg = ZenohApiConfig::default();
        let display = format!("{cfg}");
        assert!(display.contains("enabled=false"));
        assert!(display.contains(&format!("bucket={DEFAULT_BUCKET}")));
        assert!(display.contains("config=<none>"));
        assert!(display.contains("config_path=<none>"));
        assert!(display.contains("sub_keyexprs=<disabled>"));
        assert!(display.contains("query_keyexprs=<disabled>"));
        assert!(display.contains("query_locality=Any"));
        assert!(display.contains("tls=false"));
    }
}
