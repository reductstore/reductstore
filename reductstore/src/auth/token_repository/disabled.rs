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

    fn get_token(&self, _name: &str) -> Result<&Token, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    fn get_mut_token(&mut self, _name: &str) -> Result<&mut Token, ReductError> {
        Err(bad_request!("Authentication is disabled"))
    }

    fn get_token_list(&self) -> Result<Vec<Token>, ReductError> {
        Ok(vec![])
    }

    fn validate_token(&self, _header: Option<&str>) -> Result<Token, ReductError> {
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
