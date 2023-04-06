// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::core::status::HTTPError;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/reduct.proto.api.rs"));
}

use proto::token::Permissions;

/// Policy is a trait that defines the interface for a policy.
/// A policy is a set of rules that are applied to a token to determine
/// if the token is allowed to perform an action.
pub trait Policy {
    /// Validate validates a token against the policy.
    /// If the token is valid, the function returns Ok(()).
    /// If the token is invalid, the function returns an HTTPError.
    /// The HTTPError should contain the status code and message to be returned to the client.
    ///
    /// # Arguments
    /// * `permissions` - The permissions of the token to validate.
    ///
    /// # Returns
    /// * `Result<(), HTTPError>` - The result of the validation.
    fn validate(&self, permissions: Result<Permissions, HTTPError>) -> Result<(), HTTPError>;
}


/// AnonymousPolicy validates any token and even if the token is invalid.
pub struct AnonymousPolicy {}

impl Policy for AnonymousPolicy {
    fn validate(&self, _: Result<Permissions, HTTPError>) -> Result<(), HTTPError> {
        // Allow any token to perform any action.
        Ok(())
    }
}

/// FullAccessPolicy validates a token that has full access.
pub struct FullAccessPolicy {}

impl Policy for FullAccessPolicy {
    fn validate(&self, permissions: Result<Permissions, HTTPError>) -> Result<(), HTTPError> {
        if permissions?.full_access {
            Ok(())
        } else {
            Err(HTTPError::forbidden("Token doesn't have full access"))
        }
    }
}

/// ReadAccessPolicy validates a token that has read access for a certain bucket
pub struct ReadAccessPolicy {
    bucket: String,
}

impl Policy for ReadAccessPolicy {
    fn validate(&self, permissions: Result<Permissions, HTTPError>) -> Result<(), HTTPError> {
        let permissions = &permissions?;
        if permissions.full_access {
            return Ok(());
        }

        for bucket in &permissions.read {
            if bucket == &self.bucket {
                return Ok(());
            }
        }

        Err(HTTPError::forbidden(format!("Token doesn't have read access for the {} bucket",
                                         self.bucket).as_str()))
    }
}

/// WriteAccessPolicy validates a token that has write access for a certain bucket
pub struct WriteAccessPolicy {
    bucket: String,
}

impl Policy for WriteAccessPolicy {
    fn validate(&self, permissions: Result<Permissions, HTTPError>) -> Result<(), HTTPError> {
        let permissions = &permissions?;
        if permissions.full_access {
            return Ok(());
        }

        for bucket in &permissions.write {
            if bucket == &self.bucket {
                return Ok(());
            }
        }

        Err(HTTPError::forbidden(format!("Token doesn't have write access for the {} bucket",
                                         self.bucket).as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anonymous_policy() {
        let policy = AnonymousPolicy {};
        assert!(policy.validate(Ok(Permissions::default())).is_ok());
        assert!(policy.validate(Err(HTTPError::forbidden("Invalid token"))).is_ok());
    }

    #[test]
    fn test_full_access_policy() {
        let policy = FullAccessPolicy {};
        assert!(policy.validate(Ok(Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        })).is_ok());

        assert_eq!(policy.validate(Ok(Permissions {
            full_access: false,
            read: vec![],
            write: vec![],
        })), Err(HTTPError::forbidden("Token doesn't have full access")));

        assert_eq!(policy.validate(Err(HTTPError::forbidden("Invalid token")))
                   , Err(HTTPError::forbidden("Invalid token")));
    }

    #[test]
    fn test_read_access_policy() {
        let policy = ReadAccessPolicy { bucket: "bucket".to_string() };
        assert!(policy.validate(Ok(Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        })).is_ok());

        assert!(policy.validate(Ok(Permissions {
            full_access: false,
            read: vec!["bucket".to_string()],
            write: vec![],
        })).is_ok());

        assert_eq!(policy.validate(Ok(Permissions {
            full_access: false,
            read: vec!["bucket2".to_string()],
            write: vec![],
        })), Err(HTTPError::forbidden("Token doesn't have read access for the bucket bucket")));

        assert_eq!(policy.validate(Err(HTTPError::forbidden("Invalid token")))
                   , Err(HTTPError::forbidden("Invalid token")));
    }

    #[test]
    fn test_write_access_policy() {
        let policy = WriteAccessPolicy { bucket: "bucket".to_string() };
        assert!(policy.validate(Ok(Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        })).is_ok());

        assert!(policy.validate(Ok(Permissions {
            full_access: false,
            read: vec![],
            write: vec!["bucket".to_string()],
        })).is_ok());

        assert_eq!(policy.validate(Ok(Permissions {
            full_access: false,
            read: vec![],
            write: vec!["bucket2".to_string()],
        })), Err(HTTPError::forbidden("Token doesn't have write access for the bucket bucket")));

        assert_eq!(policy.validate(Err(HTTPError::forbidden("Invalid token")))
                   , Err(HTTPError::forbidden("Invalid token")));
    }
}
