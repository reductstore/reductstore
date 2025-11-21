// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::token_repository::{BoxedTokenRepository, ManageTokens, TokenRepositoryBuilder};
use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use log::{error, info, warn};
use reduct_base::error::ErrorCode;
use reduct_base::msg::token_api::{Permissions, Token};
use std::collections::HashMap;
use std::path::PathBuf;

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub(in crate::cfg) fn provision_tokens(&self, data_path: &PathBuf) -> BoxedTokenRepository {
        let mut token_repo =
            TokenRepositoryBuilder::new(self.cfg.clone()).build(PathBuf::from(data_path));

        for (name, token) in &self.cfg.tokens {
            let is_generated = match token_repo
                .generate_token(&name, token.permissions.clone().unwrap_or_default())
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
                let update_token = token_repo.get_mut_token(&name).unwrap();
                update_token.clone_from(token);
                update_token.is_provisioned = true;

                info!(
                    "Provisioned token '{}' with {:?}",
                    update_token.name, update_token.permissions
                );
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
        }

        tokens
            .into_iter()
            .map(|(_, token)| (token.name.clone(), token))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use reduct_base::error::ReductError;
    use reduct_base::not_found;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::default::Default;
    use std::env::VarError;

    #[rstest]
    #[tokio::test]
    async fn test_tokens(mut env_with_tokens: MockEnvGetter) {
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
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0");
        let components = cfg.build().unwrap();

        let repo = components.token_repo.read().await;
        let token1 = repo.get_token("token1").unwrap().clone();
        assert_eq!(token1.value, "TOKEN");
        assert!(token1.is_provisioned);

        let permissions = token1.permissions.unwrap();
        assert_eq!(permissions.full_access, true);
        assert_eq!(permissions.read, vec!["bucket1", "bucket2"]);
        assert_eq!(permissions.write, vec!["bucket1"]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_tokens_not_full_permissions(mut env_with_tokens: MockEnvGetter) {
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

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0");
        let components = cfg.build().unwrap();

        let repo = components.token_repo.read().await;
        let token1 = repo.get_token("token1").unwrap().clone();
        assert_eq!(token1.value, "TOKEN");
        assert!(token1.is_provisioned);

        let permissions = token1.permissions.unwrap();
        assert_eq!(permissions.full_access, true);
        assert_eq!(permissions.read, vec!["bucket1", "bucket2"]);
        assert!(permissions.write.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_tokens_no_value(mut env_with_tokens: MockEnvGetter) {
        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Err(VarError::NotPresent));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0");
        let components = cfg.build().unwrap();

        let repo = components.token_repo.read().await;
        let err = repo.get_token("token1").err().unwrap();
        assert_eq!(err, not_found!("Token 'token1' doesn't exist"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_override_token(mut env_with_tokens: MockEnvGetter) {
        let mut auth_repo =
            create_token_repository(env_with_tokens.get("RS_DATA_PATH").unwrap().into(), "XXX");
        let _ = auth_repo
            .generate_token("token1", Permissions::default())
            .unwrap();

        env_with_tokens
            .expect_get()
            .with(eq("RS_TOKEN_1_VALUE"))
            .return_const(Ok("TOKEN".to_string()));
        env_with_tokens
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::from_env(env_with_tokens, "0.0.0");
        let components = cfg.build().unwrap();

        let repo = components.token_repo.read().await;
        let token = repo.get_token("token1").unwrap();
        assert_eq!(token.value, "TOKEN");
    }

    #[fixture]
    fn env_with_tokens() -> MockEnvGetter {
        let tmp = tempfile::tempdir().unwrap();
        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(tmp.keep().to_str().unwrap().to_string()));
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
