// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::auth::token_repository::{
    BoxedTokenRepository, TokenRepositoryBuilder, INIT_TOKEN_NAME,
};
use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use crate::storage::engine::StorageEngine;
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use reduct_base::error::ErrorCode;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateRequest};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(in crate::cfg) async fn provision_tokens(
        &self,
        data_path: &PathBuf,
        storage: Arc<StorageEngine>,
    ) -> BoxedTokenRepository {
        let mut token_repo = TokenRepositoryBuilder::new(self.cfg.clone())
            .build_with_storage(PathBuf::from(data_path), storage)
            .await;

        for (name, token) in &self.cfg.tokens {
            // Try to generate token, just for name check and to create the token file if it doesn't exist.
            // If the token already exists, we will update it with the provided value and permissions.
            let is_generated = match token_repo
                .generate_token(
                    &name,
                    TokenCreateRequest {
                        permissions: token.permissions.clone().unwrap_or_default(),
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: token.ip_allowlist.clone(),
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            };

            if let Err(err) = is_generated {
                error!("Failed to provision token '{}': {}", name, err);
            } else {
                let mut update_token = token_repo
                    .get_token(&name)
                    .await
                    .expect("Token must exist after generation")
                    .clone();
                update_token.clone_from(token);
                update_token.is_provisioned = true;
                if let Err(err) = token_repo.update_token(update_token.clone()).await {
                    error!("Failed to persist provisioned token '{}': {}", name, err);
                    continue;
                }

                info!(
                    "Provisioned token '{}' with {:?}",
                    update_token.name, update_token.permissions
                );
            }
        }

        // Deprovision tokens that are not in the config but exist in the repo
        let existing_tokens = token_repo.get_token_list().await.unwrap_or_default();
        for existing_token in existing_tokens {
            if !self.cfg.tokens.contains_key(&existing_token.name) {
                if existing_token.is_provisioned && existing_token.name != INIT_TOKEN_NAME {
                    // we need to get again, since list returns tokens without value
                    let mut deprovisioned_token = token_repo
                        .get_token(&existing_token.name)
                        .await
                        .expect("Token from the repository list must exist")
                        .clone();
                    deprovisioned_token.is_provisioned = false;
                    if let Err(err) = token_repo.update_token(deprovisioned_token).await {
                        error!(
                            "Failed to deprovision token '{}': {}",
                            existing_token.name, err
                        );
                        continue;
                    }

                    info!("Deprovisioned token '{}'", existing_token.name);
                }
            }
        }
        token_repo
    }

    pub(in crate::cfg) fn parse_tokens(env: &mut Env<EnvGetter>) -> HashMap<String, Token> {
        let mut tokens = HashMap::<String, Token>::new();
        for (id, name) in env.matches("RS_TOKEN_(.*)_NAME") {
            let token = Token {
                name,
                created_at: chrono::Utc::now(),
                ..Token::default()
            };
            tokens.insert(id, token);
        }

        for (id, token) in &mut tokens {
            token.value =
                env.get_masked::<String>(&format!("RS_TOKEN_{}_VALUE", id), "".to_string());
        }

        tokens.retain(|_, token| {
            if token.value.is_empty() {
                warn!("Token '{}' has no value. Drop it.", token.name);
                false
            } else {
                true
            }
        });

        let parse_list_env = |env: &mut Env<EnvGetter>, name: String| -> Vec<String> {
            env.get_optional::<String>(&name)
                .unwrap_or_default()
                .split(",")
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };

        // Parse permissions
        for (id, token) in &mut tokens {
            let read = parse_list_env(env, format!("RS_TOKEN_{}_READ", id));
            let write = parse_list_env(env, format!("RS_TOKEN_{}_WRITE", id));
            let permissions = Permissions {
                full_access: env
                    .get_optional(&format!("RS_TOKEN_{}_FULL_ACCESS", id))
                    .unwrap_or_default(),
                read,
                write,
            };

            token.permissions = Some(permissions);

            if let Some(expires_at) =
                env.get_optional::<String>(&format!("RS_TOKEN_{}_EXPIRES_AT", id))
            {
                match DateTime::parse_from_rfc3339(&expires_at) {
                    Ok(value) => token.expires_at = Some(value.with_timezone(&Utc)),
                    Err(_) => warn!(
                        "Token '{}' has invalid expiry date '{}'. Ignore it.",
                        token.name, expires_at
                    ),
                }
            }
        }

        tokens
            .into_iter()
            .map(|(_, token)| (token.name.clone(), token))
            .collect()
    }
}

#[cfg(test)]
#[cfg(not(windows))] // fixme: Windows paths differ in tests
mod tests {
    use super::*;
    use crate::auth::token_secret::{is_hashed_token_secret, verify_token_secret};

    use crate::cfg::tests::MockEnvGetter;
    use crate::cfg::Cfg;
    use crate::core::env::Env;
    use crate::core::file_cache::FILE_CACHE;

    use mockall::predicate::eq;
    use reduct_base::error::ReductError;
    use reduct_base::msg::token_api::{Permissions, Token};
    use reduct_base::not_found;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use std::collections::BTreeMap;
    use std::default::Default;
    use std::env::VarError;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_tokens(#[future] env_with_tokens: MockEnvGetter) {
        let mut env_with_tokens = env_with_tokens.await;
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_FULL_ACCESS"))
            .return_const(Ok("true".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_READ"))
            .return_const(Ok("bucket1,bucket2".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_WRITE"))
            .return_const(Ok("bucket1".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_EXPIRES_AT"))
            .return_const(Ok("2030-01-01T00:00:00Z".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let components = cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let token1 = repo.get_token("token1").await.unwrap().clone();
        assert!(is_hashed_token_secret(&token1.value));
        assert!(verify_token_secret(&token1.value, "TOKEN"));
        assert!(token1.is_provisioned);

        let permissions = token1.permissions.unwrap();
        assert_eq!(permissions.full_access, true);
        assert_eq!(permissions.read, vec!["bucket1", "bucket2"]);
        assert_eq!(permissions.write, vec!["bucket1"]);
        assert_eq!(
            token1.expires_at,
            Some(
                DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_tokens_not_full_permissions(#[future] env_with_tokens: MockEnvGetter) {
        let mut env_with_tokens = env_with_tokens.await;
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_FULL_ACCESS"))
            .return_const(Ok("true".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_READ"))
            .return_const(Ok("bucket1,bucket2".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let components = cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let token1 = repo.get_token("token1").await.unwrap().clone();
        assert!(is_hashed_token_secret(&token1.value));
        assert!(verify_token_secret(&token1.value, "TOKEN"));
        assert!(token1.is_provisioned);

        let permissions = token1.permissions.unwrap();
        assert_eq!(permissions.full_access, true);
        assert_eq!(permissions.read, vec!["bucket1", "bucket2"]);
        assert!(permissions.write.is_empty());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_tokens_no_value(#[future] env_with_tokens: MockEnvGetter) {
        let mut env_with_tokens = env_with_tokens.await;
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Err(VarError::NotPresent));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let components = cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let err = repo.get_token("token1").await.err().unwrap();
        assert_eq!(err, not_found!("Token 'token1' doesn't exist"));
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_override_token(#[future] env_with_tokens: MockEnvGetter) {
        let mut env_with_tokens = env_with_tokens.await;
        let data_path = env_with_tokens.get("RS_DATA_PATH").unwrap();
        let mut auth_repo = TokenRepositoryBuilder::new(Cfg {
            api_token: "init".to_string(),
            ..Default::default()
        })
        .build(data_path.clone().into())
        .await;
        let _ = auth_repo
            .generate_token(
                "token1",
                TokenCreateRequest {
                    permissions: Permissions::default(),
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .unwrap();

        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let components = cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let token = repo.get_token("token1").await.unwrap();
        assert!(is_hashed_token_secret(&token.value));
        assert!(verify_token_secret(&token.value, "TOKEN"));

        drop(repo);

        let mut repo = TokenRepositoryBuilder::new(Cfg {
            api_token: "XXX".to_string(),
            ..Default::default()
        })
        .build(data_path.into())
        .await;
        let token = repo.get_token("token1").await.unwrap();
        assert!(is_hashed_token_secret(&token.value));
        assert!(verify_token_secret(&token.value, "TOKEN"));
        assert!(token.is_provisioned);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_override_token_keeps_previous_version_when_update_fails(
        #[future] env_with_tokens: MockEnvGetter,
    ) {
        let mut env_with_tokens = env_with_tokens.await;
        let data_path = PathBuf::from(env_with_tokens.get("RS_DATA_PATH").unwrap());

        let mut auth_repo = TokenRepositoryBuilder::new(Cfg {
            api_token: "init".to_string(),
            ..Default::default()
        })
        .build(data_path.clone())
        .await;
        auth_repo
            .generate_token(
                "token1",
                TokenCreateRequest {
                    permissions: Permissions::default(),
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .unwrap();
        drop(auth_repo);
        FILE_CACHE.discard_recursive(&data_path).await.unwrap();

        let token_file = data_path.join(".auth");
        fs::set_permissions(&data_path, fs::Permissions::from_mode(0o555)).unwrap();
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o444)).unwrap();

        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let _components = cfg.build().await.unwrap();

        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&data_path, fs::Permissions::from_mode(0o755)).unwrap();

        let mut repo = TokenRepositoryBuilder::new(Cfg {
            api_token: "XXX".to_string(),
            ..Default::default()
        })
        .build(data_path)
        .await;
        let token = repo.get_token("token1").await.unwrap();
        assert!(!verify_token_secret(&token.value, "TOKEN"));
        assert!(!token.is_provisioned);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_deprovision_tokens_removed_from_config(#[future] env_with_tokens: MockEnvGetter) {
        let mut env_with_tokens = env_with_tokens.await;
        let data_path = PathBuf::from(env_with_tokens.get("RS_DATA_PATH").unwrap());

        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let _components = cfg.build().await.unwrap();
        FILE_CACHE.discard_recursive(&data_path).await.unwrap();

        let empty_cfg = cfg_parser_with_tokens(&data_path, HashMap::new());
        let components = empty_cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let token = repo.get_token("token1").await.unwrap();
        assert!(is_hashed_token_secret(&token.value));
        assert!(verify_token_secret(&token.value, "TOKEN"));
        assert!(!token.is_provisioned);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_keeps_init_token_provisioned_when_not_in_config(
        #[future] env_with_tokens: MockEnvGetter,
    ) {
        let mut env_with_tokens = env_with_tokens.await;
        let data_path = PathBuf::from(env_with_tokens.get("RS_DATA_PATH").unwrap());

        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let _components = cfg.build().await.unwrap();
        FILE_CACHE.discard_recursive(&data_path).await.unwrap();

        let empty_cfg = cfg_parser_with_tokens(&data_path, HashMap::new());
        let components = empty_cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let init_token = repo.get_token(INIT_TOKEN_NAME).await.unwrap();
        assert!(init_token.is_provisioned);
        assert!(is_hashed_token_secret(&init_token.value));
        assert!(verify_token_secret(&init_token.value, "XXX"));
    }

    fn cfg_parser_with_tokens(
        data_path: &PathBuf,
        tokens: HashMap<String, Token>,
    ) -> CfgParser<MockEnvGetter> {
        CfgParser {
            cfg: Cfg {
                data_path: data_path.clone(),
                api_token: "XXX".to_string(),
                tokens,
                ..Default::default()
            },
            license: None,
            env: Env::new(MockEnvGetter::new()),
            ext_cfg: crate::cfg::CoreExtCfg {
                role: crate::cfg::InstanceRole::Primary,
                data_path: data_path.clone(),
            },
        }
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_tokens_invalid_expiry(#[future] env_with_tokens: MockEnvGetter) {
        let mut env_with_tokens = env_with_tokens.await;
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_EXPIRES_AT"))
            .return_const(Ok("invalid-date".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0").await;
        let components = cfg.build().await.unwrap();

        let mut repo = components.token_repo.write().await.unwrap();
        let token1 = repo.get_token("token1").await.unwrap().clone();
        assert!(is_hashed_token_secret(&token1.value));
        assert!(verify_token_secret(&token1.value, "TOKEN"));
        assert_eq!(token1.expires_at, None);
    }

    #[fixture]
    async fn env_with_tokens() -> MockEnvGetter {
        let tmp = tempfile::tempdir().unwrap();
        let data_path = tmp.keep();
        fs::create_dir_all(&data_path).unwrap();

        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(data_path.to_str().unwrap().to_string()));
        mock_getter
            .expect_get()
            .with(eq("RS_API_TOKEN"))
            .return_const(Ok("XXX".to_string()));
        mock_getter.expect_all().returning(|| {
            let mut map = BTreeMap::new();
            map.insert("RS_TOKEN_1_NAME".to_string(), "token1".to_string());
            map
        });
        mock_getter
            .expect_get()
            .with(eq("RS_TOKEN_1_NAME"))
            .return_const(Ok("token1".to_string()));
        mock_getter
    }
}
