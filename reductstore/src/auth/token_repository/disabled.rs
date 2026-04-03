// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::auth::token_repository::ManageTokens;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reduct_base::bad_request;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateRequest, TokenCreateResponse};
use std::net::IpAddr;
use std::time::SystemTime;

/// A repository that doesn't require authentication
pub(super) struct NoAuthRepository {}

impl NoAuthRepository {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ManageTokens for NoAuthRepository {
    async fn generate_token(
        &mut self,
        _name: &str,
        _request: TokenCreateRequest,
    ) -> Result<TokenCreateResponse, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    async fn get_token(&mut self, _name: &str) -> Result<&Token, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    async fn update_token(&mut self, _token: Token) -> Result<(), ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    async fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        Ok(vec![])
    }

    async fn validate_token(
        &mut self,
        _header: Option<&str>,
        _client_ip: Option<IpAddr>,
    ) -> Result<Token, ReductError> {
        Ok(Token {
            name: "AUTHENTICATION-DISABLED".to_string(),
            value: "".to_string(),
            created_at: DateTime::<Utc>::from(SystemTime::now()),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
                ip_allowlist: vec![],
            }),
            is_provisioned: false,
            expires_at: None,
            ttl: None,
            last_access: None,
            is_expired: false,
        })
    }

    async fn remove_token(&mut self, _name: &str) -> Result<(), ReductError> {
        Ok(())
    }

    async fn rotate_token(&mut self, _name: &str) -> Result<TokenCreateResponse, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    async fn remove_bucket_from_tokens(&mut self, _bucket: &str) -> Result<(), ReductError> {
        Ok(())
    }

    async fn rename_bucket(&mut self, _old_name: &str, _new_name: &str) -> Result<(), ReductError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_repository::{BoxedTokenRepository, TokenRepositoryBuilder};
    use reduct_base::bad_request;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_create_token_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let token = disabled_repo
            .generate_token(
                "test",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                        ip_allowlist: vec![],
                    },
                    expires_at: None,

                    ttl: None,
                },
            )
            .await;
        assert_eq!(token, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    #[tokio::test]
    async fn test_find_by_name_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let token = disabled_repo.get_token("test").await;
        assert_eq!(token, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_token_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let result = disabled_repo.update_token(Token::default()).await;
        assert_eq!(result, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_list_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let token_list = disabled_repo.get_token_list().await.unwrap();
        assert_eq!(token_list, vec![]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_validate_token_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let placeholder = disabled_repo
            .validate_token(Some("invalid-value"), None)
            .await
            .unwrap();
        assert_eq!(placeholder.name, "AUTHENTICATION-DISABLED");
        assert_eq!(placeholder.value, "");
        assert!(placeholder.permissions.unwrap().full_access);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_token_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let token = disabled_repo.remove_token("test").await;
        assert_eq!(token, Ok(()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_rotate_token_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let result = disabled_repo.rotate_token("test").await;
        assert_eq!(result, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket_no_init_token(#[future] disabled_repo: BoxedTokenRepository) {
        let mut disabled_repo = disabled_repo.await;
        let result = disabled_repo.rename_bucket("bucket-1", "bucket-2").await;
        assert!(result.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_from_tokens_no_init_token(
        #[future] disabled_repo: BoxedTokenRepository,
    ) {
        let mut disabled_repo = disabled_repo.await;
        let result = disabled_repo.remove_bucket_from_tokens("bucket-1").await;
        assert!(result.is_ok());
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    async fn disabled_repo(path: PathBuf) -> BoxedTokenRepository {
        TokenRepositoryBuilder::new(Default::default())
            .build(path.clone())
            .await
    }
}
