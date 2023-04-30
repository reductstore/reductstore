// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::Policy;
use crate::auth::token_repository::TokenRepository;
use crate::core::status::HttpError;

/// Authorization by token
pub struct TokenAuthorization {
    api_token: String,
}

impl TokenAuthorization {
    /// Create a new TokenAuthorization
    ///
    /// # Arguments
    /// * `api_token` - The API token to use for authorization. If it is empty, no authorization is required.
    ///
    /// # Returns
    /// * `TokenAuthorization` - The new TokenAuthorization
    pub fn new(api_token: &str) -> Self {
        Self {
            api_token: api_token.to_string(),
        }
    }

    /// Check if the request is authorized.
    ///
    /// # Arguments
    /// * `authorization_header` - The value of the Authorization header.
    /// * `repo` - The token repository to validate the token value.
    /// * `policy` - The policy to validate the token permissions.
    pub fn check<Plc: Policy>(
        &self,
        authorization_header: Option<&str>,
        repo: &TokenRepository,
        policy: Plc,
    ) -> Result<(), HttpError> {
        if self.api_token.is_empty() {
            // No API token set, so no authorization is required.
            return Ok(());
        }

        let token = repo.validate_token(authorization_header);
        policy.validate(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::policy::{AnonymousPolicy, FullAccessPolicy};
    use tempfile::tempdir;

    #[test]
    fn test_anonymous_policy() {
        let (repo, auth) = setup();
        let result = auth.check(Some("invalid"), &repo, AnonymousPolicy {});

        assert!(result.is_ok());

        let result = auth.check(Some("Bearer invalid"), &repo, AnonymousPolicy {});

        assert!(result.is_ok());

        let result = auth.check(Some("Bearer test"), &repo, AnonymousPolicy {});
        assert!(result.is_ok());
    }

    #[test]
    fn test_full_access_policy() {
        let (repo, auth) = setup();
        let result = auth.check(Some("invalid"), &repo, FullAccessPolicy {});

        assert_eq!(
            result,
            Err(HttpError::unauthorized("No bearer token in request header"))
        );

        let result = auth.check(Some("Bearer invalid"), &repo, FullAccessPolicy {});
        assert_eq!(result, Err(HttpError::unauthorized("Invalid token")));

        let result = auth.check(Some("Bearer test"), &repo, FullAccessPolicy {});
        assert!(result.is_ok());
    }

    fn setup() -> (TokenRepository, TokenAuthorization) {
        let repo = TokenRepository::new(tempdir().unwrap().into_path(), "test");
        let auth = TokenAuthorization::new("test");

        (repo, auth)
    }
}
