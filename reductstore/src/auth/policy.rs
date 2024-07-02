// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1
use reduct_base::error::ReductError;

use reduct_base::msg::token_api::Token;

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
        if token?
            .permissions
            .ok_or(ReductError::internal_server_error("No permissions set"))?
            .full_access
        {
            Ok(())
        } else {
            Err(ReductError::forbidden("Token doesn't have full access"))
        }
    }
}

/// ReadAccessPolicy validates a token that has read access for a certain bucket
pub struct ReadAccessPolicy {
    pub(crate) bucket: String,
}

impl Policy for ReadAccessPolicy {
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError> {
        let permissions = &token?
            .permissions
            .ok_or(ReductError::internal_server_error("No permissions set"))?;

        if permissions.full_access {
            return Ok(());
        }

        for bucket in &permissions.read {
            if bucket == &self.bucket {
                return Ok(());
            }
        }

        Err(ReductError::forbidden(
            format!(
                "Token doesn't have read access for the {} bucket",
                self.bucket
            )
            .as_str(),
        ))
    }
}

/// WriteAccessPolicy validates a token that has write access for a certain bucket
pub struct WriteAccessPolicy {
    pub(crate) bucket: String,
}

impl Policy for WriteAccessPolicy {
    fn validate(&self, token: Result<Token, ReductError>) -> Result<(), ReductError> {
        let permissions = &token?
            .permissions
            .ok_or(ReductError::internal_server_error("No permissions set"))?;

        if permissions.full_access {
            return Ok(());
        }

        for bucket in &permissions.write {
            if bucket == &self.bucket {
                return Ok(());
            }
        }

        Err(ReductError::forbidden(
            format!(
                "Token doesn't have write access for the {} bucket",
                self.bucket
            )
            .as_str(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::msg::token_api::Permissions;

    #[test]
    fn test_anonymous_policy() {
        let policy = AnonymousPolicy {};
        assert!(policy.validate(Ok(Token::default())).is_ok());
        assert!(policy
            .validate(Err(ReductError::forbidden("Invalid token")))
            .is_ok());
    }

    #[test]
    fn test_authenticated_policy() {
        let policy = AuthenticatedPolicy {};
        assert!(policy.validate(Ok(Token::default())).is_ok());
        assert_eq!(
            policy.validate(Err(ReductError::forbidden("Invalid token"))),
            Err(ReductError::forbidden("Invalid token"))
        );
    }

    #[test]
    fn test_full_access_policy() {
        let policy = FullAccessPolicy {};
        let mut token = Token {
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
            Err(ReductError::forbidden("Token doesn't have full access"))
        );

        assert_eq!(
            policy.validate(Err(ReductError::forbidden("Invalid token"))),
            Err(ReductError::forbidden("Invalid token"))
        );
    }

    #[test]
    fn test_read_access_policy() {
        let policy = ReadAccessPolicy {
            bucket: "bucket".to_string(),
        };
        let mut token = Token {
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
            policy.validate(Ok(token)),
            Err(ReductError::forbidden(
                "Token doesn't have read access for the bucket bucket"
            ))
        );

        assert_eq!(
            policy.validate(Err(ReductError::forbidden("Invalid token"))),
            Err(ReductError::forbidden("Invalid token"))
        );
    }

    #[test]
    fn test_write_access_policy() {
        let policy = WriteAccessPolicy {
            bucket: "bucket".to_string(),
        };
        let mut token = Token {
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
            policy.validate(Ok(token)),
            Err(ReductError::forbidden(
                "Token doesn't have write access for the bucket bucket"
            ))
        );

        assert_eq!(
            policy.validate(Err(ReductError::forbidden("Invalid token"))),
            Err(ReductError::forbidden("Invalid token"))
        );
    }
}
