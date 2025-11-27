// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::token_repository::ManageTokens;
use chrono::{DateTime, Utc};
use reduct_base::bad_request;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
use std::time::SystemTime;

/// A repository that doesn't require authentication
pub(super) struct NoAuthRepository {}

impl NoAuthRepository {
    pub fn new() -> Self {
        Self {}
    }
}

impl ManageTokens for NoAuthRepository {
    fn generate_token(
        &mut self,
        _name: &str,
        _permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    fn get_token(&mut self, _name: &str) -> Result<&Token, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    fn get_mut_token(&mut self, _name: &str) -> Result<&mut Token, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        Ok(vec![])
    }

    fn validate_token(&mut self, _header: Option<&str>) -> Result<Token, ReductError> {
        Ok(Token {
            name: "AUTHENTICATION-DISABLED".to_string(),
            value: "".to_string(),
            created_at: DateTime::<Utc>::from(SystemTime::now()),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            is_provisioned: false,
        })
    }

    fn remove_token(&mut self, _name: &str) -> Result<(), ReductError> {
        Ok(())
    }

    fn remove_bucket_from_tokens(&mut self, _bucket: &str) -> Result<(), ReductError> {
        Ok(())
    }

    fn rename_bucket(&mut self, _old_name: &str, _new_name: &str) -> Result<(), ReductError> {
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
    fn test_create_token_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let token = disabled_repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );
        assert_eq!(token, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    fn test_find_by_name_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let token = disabled_repo.get_token("test");
        assert_eq!(token, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    fn test_find_mut_by_name_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let token = disabled_repo.get_mut_token("test");
        assert_eq!(token, Err(bad_request!("Authentication is disabled")));
    }

    #[rstest]
    fn test_get_token_list_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let token_list = disabled_repo.get_token_list().unwrap();
        assert_eq!(token_list, vec![]);
    }

    #[rstest]
    fn test_validate_token_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let placeholder = disabled_repo.validate_token(Some("invalid-value")).unwrap();
        assert_eq!(placeholder.name, "AUTHENTICATION-DISABLED");
        assert_eq!(placeholder.value, "");
        assert_eq!(placeholder.permissions.unwrap().full_access, true);
    }

    #[rstest]
    fn test_remove_token_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let token = disabled_repo.remove_token("test");
        assert_eq!(token, Ok(()));
    }

    #[rstest]
    fn test_rename_bucket_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let result = disabled_repo.rename_bucket("bucket-1", "bucket-2");
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_remove_bucket_from_tokens_no_init_token(mut disabled_repo: BoxedTokenRepository) {
        let result = disabled_repo.remove_bucket_from_tokens("bucket-1");
        assert!(result.is_ok());
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    fn disabled_repo(path: PathBuf) -> BoxedTokenRepository {
        TokenRepositoryBuilder::new(Default::default()).build(path.clone())
    }
}
