// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
use reduct_base::error::ReductError;

use reduct_base::msg::token_api::{Permissions, Token};
use reduct_base::{forbidden, unprocessable_entity};

/// Policy is a trait that defines the interface for a policy.
/// A policy is a set of rules that are applied to a token to determine
/// if the token is allowed to perform an action.
pub trait Policy {
    /// Validate validates a token against the policy.
    /// If the token is valid, the function returns Ok(()).
    /// If the token is invalid, the function returns an HTTPError.
    /// The HTTPError should contain the status code and message to be returned to the reduct_rs.
    ///
    /// # Arguments
    /// * `token` - The token to validate.
    ///
    /// # Returns
    /// * `Result<(), HTTPError>` - The result of the validation.
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError>;
}

/// AnonymousPolicy validates any token and even if the token is invalid.
pub struct AnonymousPolicy {}

impl Policy for AnonymousPolicy {
    fn validate(&self, _: Result<Token, ReductError>) -> Result<(), ReductError> {
        // Allow any token to perform any action.
        Ok(())
    }
}

/// AuthenticatedPolicy validates a token at least is valid.
pub struct AuthenticatedPolicy {}

impl Policy for AuthenticatedPolicy {
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError> {
        token?;
        Ok(())
    }
}

/// FullAccessPolicy validates a token that has full access.
pub struct FullAccessPolicy {}

impl Policy for FullAccessPolicy {
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError> {
        let token = token?;
        if token.permissions.unwrap_or_default().full_access {
            Ok(())
        } else {
            Err(forbidden!(
                "Token '{}' doesn't have full access",
                token.name
            ))
        }
    }
}

/// ReadAccessPolicy validates a token that has read access for a certain bucket
pub struct ReadAccessPolicy<'a> {
    pub(crate) bucket: &'a str,
}

impl Policy for ReadAccessPolicy<'_> {
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError> {
        let token = token?;
        let permissions = &token.permissions.unwrap_or_default();

        if permissions.full_access {
            return Ok(());
        }

        if check_bucket_permissions(&permissions.read, self.bucket) {
            return Ok(());
        }

        Err(forbidden!(
            "Token '{}' doesn't have read access to bucket '{}'",
            token.name,
            self.bucket
        ))
    }
}

/// WriteAccessPolicy validates a token that has write access for a certain bucket
pub struct WriteAccessPolicy<'a> {
    pub(crate) bucket: &'a str,
}

impl Policy for WriteAccessPolicy<'_> {
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError> {
        let token = token?;
        let permissions = &token.permissions.unwrap_or_default();

        if permissions.full_access {
            return Ok(());
        }

        if check_bucket_permissions(&permissions.write, self.bucket) {
            return Ok(());
        }

        Err(forbidden!(
            "Token '{}' doesn't have write access to bucket '{}'",
            token.name,
            self.bucket
        ))
    }
}

fn check_bucket_permissions(token_list: &Vec<String>, bucket: &str) -> bool {
    for token_bucket in token_list {
        // Check if the token has access for the specified bucket with wildcard support
        let wildcard_bucket = token_bucket.ends_with('*');
        if token_bucket == bucket
            || (wildcard_bucket && bucket.starts_with(&token_bucket[..token_bucket.len() - 1]))
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::msg::token_api::Permissions;

    #[test]
    fn test_anonymous_policy() {
        let policy = AnonymousPolicy {};
        assert!(policy.validate(Ok(Token::default())).is_ok());
        assert!(policy.validate(Err(forbidden!("Invalid token"))).is_ok());
    }

    #[test]
    fn test_authenticated_policy() {
        let policy = AuthenticatedPolicy {};
        assert!(policy.validate(Ok(Token::default())).is_ok());
        assert_eq!(
            policy.validate(Err(forbidden!("Invalid token"))),
            Err(forbidden!("Invalid token"))
        );
    }

    #[test]
    fn test_full_access_policy() {
        let policy = FullAccessPolicy {};
        let mut token = Token {
            name: "test_token".to_string(),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            ..Default::default()
        };
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().full_access = false;
        assert_eq!(
            policy.validate(Ok(token)),
            Err(forbidden!("Token 'test_token' doesn't have full access"))
        );

        assert_eq!(
            policy.validate(Err(forbidden!("Invalid token"))),
            Err(forbidden!("Invalid token"))
        );
    }

    #[test]
    fn test_read_access_policy() {
        let policy = ReadAccessPolicy { bucket: "bucket" };
        let mut token = Token {
            name: "test_token".to_string(),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            ..Default::default()
        };
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().full_access = false;
        token.permissions.as_mut().unwrap().read = vec!["bucket".to_string()];
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().read = vec!["bucket2".to_string()];
        assert_eq!(
            policy.validate(Ok(token.clone())),
            Err(forbidden!(
                "Token 'test_token' doesn't have read access to bucket 'bucket'"
            ))
        );

        token.permissions.as_mut().unwrap().read = vec!["bucket*".to_string()];
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().read = vec!["*".to_string()];
        assert!(policy.validate(Ok(token)).is_ok());

        assert_eq!(
            policy.validate(Err(forbidden!("Invalid token"))),
            Err(forbidden!("Invalid token"))
        );
    }

    #[test]
    fn test_write_access_policy() {
        let policy = WriteAccessPolicy { bucket: "bucket" };
        let mut token = Token {
            name: "test_token".to_string(),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            ..Default::default()
        };
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().full_access = false;
        token.permissions.as_mut().unwrap().write = vec!["bucket".to_string()];
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().write = vec!["bucket2".to_string()];
        assert_eq!(
            policy.validate(Ok(token.clone())),
            Err(forbidden!(
                "Token 'test_token' doesn't have write access to bucket 'bucket'"
            ))
        );

        token.permissions.as_mut().unwrap().write = vec!["bucket*".to_string()];
        assert!(policy.validate(Ok(token.clone())).is_ok());

        token.permissions.as_mut().unwrap().write = vec!["*".to_string()];
        assert!(policy.validate(Ok(token)).is_ok());

        assert_eq!(
            policy.validate(Err(forbidden!("Invalid token"))),
            Err(forbidden!("Invalid token"))
        );
    }
}
