// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::Policy;
use crate::auth::token_repository::{parse_bearer_token, TokenRepository};
use crate::core::status::HTTPError;

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
    pub fn check(
        &self,
        authorization_header: &str,
        repo: &TokenRepository,
        policy: &dyn Policy,
    ) -> Result<(), HTTPError> {
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
        let policy = AnonymousPolicy {};
        let (repo, auth) = setup();
        let result = auth.check("invalid", &repo, &policy);

        assert!(result.is_ok());

        let result = auth.check("Bearer invalid", &repo, &policy);

        assert!(result.is_ok());

        let result = auth.check("Bearer test", &repo, &policy);
        assert!(result.is_ok());
    }

    #[test]
    fn test_full_access_policy() {
        let policy = FullAccessPolicy {};
        let (repo, auth) = setup();
        let result = auth.check("invalid", &repo, &policy);

        assert_eq!(
            result,
            Err(HTTPError::unauthorized("No bearer token in request header"))
        );

        let result = auth.check("Bearer invalid", &repo, &policy);
        assert_eq!(result, Err(HTTPError::unauthorized("Invalid token")));

        let result = auth.check("Bearer test", &repo, &policy);
        assert!(result.is_ok());
    }

    fn setup() -> (TokenRepository, TokenAuthorization) {
        let repo = TokenRepository::new(tempdir().unwrap().into_path(), Some("test".to_string()));
        let auth = TokenAuthorization::new("test");

        (repo, auth)
    }
}
