// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::proto::TokenRepo;
use crate::auth::token_repository::TokenRepoCommon;
use crate::auth::token_repository::{ManageTokens, INIT_TOKEN_NAME, TOKEN_REPO_FILE_NAME};
use crate::core::file_cache::FILE_CACHE;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::debug;
use prost::Message;
use rand::Rng;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
use reduct_base::{conflict, internal_server_error, not_found, unprocessable_entity};
use regex::Regex;
use std::collections::HashMap;
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

/// The TokenRepository trait is used to store and retrieve tokens.
pub(super) struct TokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
    permission_regex: Regex,
}

impl TokenRepository {
    /// Load the token repository from the file system
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data directory
    /// * `api_token` - The API token with full access to the repository. If it is empty, no authentication is required.
    ///
    /// # Returns
    ///
    /// The repository
    pub fn new(data_path: PathBuf, api_token: String) -> TokenRepository {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);
        let repo = HashMap::new();

        if api_token.is_empty() {
            panic!("API must be set");
        }

        // Load the token repository from the file system
        let permission_regex =
            Regex::new(r"^[*a-zA-Z0-9_\-]+$").expect("Invalid regex for permissions");
        let mut token_repository = TokenRepository {
            config_path,
            repo,
            permission_regex,
        };

        let lock = FILE_CACHE.read(&token_repository.config_path, SeekFrom::Start(0));
        match lock {
            Ok(lock) => {
                debug!(
                    "Loading token repository from {}",
                    token_repository.config_path.as_path().display()
                );

                let mut buf = Vec::new();
                lock.upgrade()
                    .unwrap()
                    .write()
                    .unwrap()
                    .read_to_end(&mut buf)
                    .expect("Could not read token repository");

                let proto_repo = TokenRepo::decode(&mut Bytes::from(buf))
                    .expect("Could not decode token repository");
                for token in proto_repo.tokens {
                    token_repository
                        .repo
                        .insert(token.name.clone(), token.into());
                }
            }
            Err(_) => {
                debug!(
                    "Creating a new token repository {}",
                    token_repository.config_path.as_path().display()
                );
                token_repository
                    .save_repo()
                    .expect("Failed to create a new token repository");
            }
        };

        let init_token = Token {
            name: INIT_TOKEN_NAME.to_string(),
            value: api_token.to_string(),
            created_at: DateTime::<Utc>::from(SystemTime::now()),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            is_provisioned: true,
        };

        token_repository
            .repo
            .insert(init_token.name.clone(), init_token);
        token_repository
    }

    /// Save the token repository to the file system
    fn save_repo(&mut self) -> Result<(), ReductError> {
        let repo = TokenRepo {
            tokens: self
                .repo
                .iter()
                .map(|(_, token)| token.clone().into())
                .collect(),
        };
        let mut buf = Vec::new();
        repo.encode(&mut buf)
            .map_err(|_| ReductError::internal_server_error("Could not encode token repository"))?;

        let lock = FILE_CACHE
            .write_or_create(&self.config_path, SeekFrom::Start(0))?
            .upgrade()?;

        let mut file = lock.write()?;
        file.set_len(0)?;
        file.write_all(&buf).map_err(|err| {
            internal_server_error!(
                "Could not write token repository to {}: {}",
                self.config_path.as_path().display(),
                err
            )
        })?;

        file.sync_all()?;
        Ok(())
    }
}

impl TokenRepoCommon for TokenRepository {
    fn repo(&self) -> &HashMap<String, Token> {
        &self.repo
    }
}

impl ManageTokens for TokenRepository {
    fn generate_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        // Check if the token isn't empty
        if name.is_empty() {
            return Err(unprocessable_entity!("Token name can't be empty"));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(conflict!("Token '{}' already exists", name));
        }

        for entry in permissions.read.iter().chain(&permissions.write) {
            if !self.permission_regex.is_match(entry) {
                return Err(unprocessable_entity!(
                    "Permission can contain only bucket names or wildcard '*', got '{}'",
                    entry
                ));
            }
        }

        let created_at = DateTime::<Utc>::from(SystemTime::now());

        // Create a random hex string
        let mut rng = rand::rng();
        let value: String = (0..32)
            .map(|_| format!("{:x}", rng.random_range(0..16)))
            .collect();
        let value = format!("{}-{}", name, value);
        let token = Token {
            name: name.to_string(),
            value: value.clone(),
            created_at: created_at.clone(),
            permissions: Some(permissions),
            is_provisioned: false,
        };

        self.repo.insert(name.to_string(), token);
        self.save_repo()?;

        Ok(TokenCreateResponse { value, created_at })
    }

    fn get_token(&mut self, name: &str) -> Result<&Token, ReductError> {
        TokenRepoCommon::get_token(self, name)
    }

    fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError> {
        match self.repo.get_mut(name) {
            Some(token) => Ok(token),
            None => Err(not_found!("Token '{}' doesn't exist", name)),
        }
    }

    fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        TokenRepoCommon::get_token_list(self)
    }

    fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError> {
        TokenRepoCommon::validate_token(self, header)
    }

    fn remove_token(&mut self, name: &str) -> Result<(), ReductError> {
        if let Some(token) = self.repo.get(name) {
            if token.is_provisioned {
                return Err(conflict!("Can't remove provisioned token '{}'", name));
            }
        }

        if self.repo.remove(name).is_none() {
            Err(not_found!("Token '{}' doesn't exist", name))
        } else {
            self.save_repo()
        }
    }

    fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions.read.retain(|b| b != bucket);
                permissions.write.retain(|b| b != bucket);
            }
        }

        self.save_repo()
    }

    fn rename_bucket(&mut self, old_name: &str, new_name: &str) -> Result<(), ReductError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions
                    .read
                    .iter_mut()
                    .filter(|b| *b == old_name)
                    .for_each(|b| {
                        *b = new_name.to_string();
                    });

                permissions
                    .write
                    .iter_mut()
                    .filter(|b| *b == old_name)
                    .for_each(|b| {
                        *b = new_name.to_string();
                    });
            }
        }

        self.save_repo()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_repository::{BoxedTokenRepository, TokenRepositoryBuilder};
    use reduct_base::{bad_request, conflict, unprocessable_entity};
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    fn test_init_token(mut repo: BoxedTokenRepository) {
        let token = repo.validate_token(Some("Bearer init-token")).unwrap();
        assert_eq!(token.name, "init-token");
        assert_eq!(token.value, "init-token");
        assert!(token.is_provisioned);

        let token_list = repo.get_token_list().unwrap();
        assert_eq!(token_list.len(), 2);
        assert_eq!(token_list[0].name, "init-token");
    }

    mod create_token {
        use super::*;

        #[rstest]
        fn test_create_empty_token(mut repo: BoxedTokenRepository) {
            let token = repo.generate_token(
                "",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            );

            assert_eq!(
                token,
                Err(unprocessable_entity!("Token name can't be empty"))
            );
        }

        #[rstest]
        fn test_create_existing_token(mut repo: BoxedTokenRepository) {
            let token = repo.generate_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            );

            assert_eq!(token, Err(conflict!("Token 'test' already exists")));
        }

        #[rstest]
        fn test_create_token(mut repo: BoxedTokenRepository) {
            let token = repo
                .generate_token(
                    "test-1",
                    Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    },
                )
                .unwrap();

            assert_eq!(token.value.len(), 39);
            assert_eq!(token.value, "test-1-".to_string() + &token.value[7..]);
            assert!(token.created_at.timestamp() > 0);
        }

        #[rstest]
        fn test_create_token_persistent(path: PathBuf) {
            let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
            repo.generate_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            )
            .unwrap();

            let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
            assert_eq!(repo.get_token("test").unwrap().name, "test");
        }

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
        #[case("*", None)]
        #[case("bucket_1", None)]
        #[case("bucket_2", None)]
        #[case("bucket-*", None)]
        #[case("%!", Some(unprocessable_entity!("Permission can contain only bucket names or wildcard '*', got '%!'")))]
        fn test_create_token_check_format_read(
            mut repo: BoxedTokenRepository,
            #[case] bucket: &str,
            #[case] expected: Option<ReductError>,
        ) {
            let token = repo.generate_token(
                "test-1",
                Permissions {
                    full_access: true,
                    read: vec![bucket.to_string()],
                    write: vec![],
                },
            );

            assert_eq!(token.err(), expected);
        }

        #[rstest]
        #[case("*", None)]
        #[case("bucket_1", None)]
        #[case("bucket_2", None)]
        #[case("bucket-*", None)]
        #[case("%!", Some(unprocessable_entity!("Permission can contain only bucket names or wildcard '*', got '%!'")))]
        fn test_create_token_check_format_write(
            mut repo: BoxedTokenRepository,
            #[case] bucket: &str,
            #[case] expected: Option<ReductError>,
        ) {
            let token = repo.generate_token(
                "test-1",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![bucket.to_string()],
                },
            );

            assert_eq!(token.err(), expected);
        }
    }

    mod find_token {
        use super::*;
        #[rstest]
        fn test_find_by_name(mut repo: BoxedTokenRepository) {
            let token = repo.get_token("test").unwrap();
            assert_eq!(token.name, "test");
            assert!(token.value.starts_with("test-"));
        }

        #[rstest]
        fn test_find_by_name_not_found(mut repo: BoxedTokenRepository) {
            let token = repo.get_token("test-1");
            assert_eq!(token, Err(not_found!("Token 'test-1' doesn't exist")));
        }

        #[rstest]
        fn test_find_by_name_no_init_token(mut disabled_repo: BoxedTokenRepository) {
            let token = disabled_repo.get_token("test");
            assert_eq!(token, Err(bad_request!("Authentication is disabled")));
        }
    }

    mod token_list {
        use super::*;
        #[rstest]
        fn test_get_token_list(mut repo: BoxedTokenRepository) {
            let token_list = repo.get_token_list().unwrap();

            assert_eq!(token_list.len(), 2);
            assert_eq!(token_list[1].name, "test");
            assert_eq!(token_list[1].value, "");
            assert_eq!(
                token_list[1].permissions,
                Some(Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                })
            );
        }

        #[rstest]
        fn test_get_token_list_no_init_token(mut disabled_repo: BoxedTokenRepository) {
            let token_list = disabled_repo.get_token_list().unwrap();
            assert_eq!(token_list, vec![]);
        }
    }

    mod validate_token {
        use super::*;
        use reduct_base::unauthorized;
        #[rstest]
        fn test_validate_token(mut repo: BoxedTokenRepository) {
            let value = repo
                .generate_token(
                    "test-1",
                    Permissions {
                        full_access: true,
                        read: vec!["bucket-1".to_string()],
                        write: vec!["bucket-2".to_string()],
                    },
                )
                .unwrap()
                .value;

            let token = repo
                .validate_token(Some(&format!("Bearer {}", value)))
                .unwrap();

            assert_eq!(
                token,
                Token {
                    name: "test-1".to_string(),
                    created_at: token.created_at.clone(),
                    value: token.value.clone(), // generated value
                    permissions: Some(Permissions {
                        full_access: true,
                        read: vec!["bucket-1".to_string()],
                        write: vec!["bucket-2".to_string()],
                    }),
                    is_provisioned: false,
                }
            );

            #[rstest]
            fn test_validate_token_not_found(mut repo: BoxedTokenRepository) {
                let token = repo.validate_token(Some("Bearer invalid-value"));
                assert_eq!(token, Err(unauthorized!("Invalid token")));
            }

            #[rstest]
            fn test_validate_token_no_init_token(mut disabled_repo: BoxedTokenRepository) {
                let placeholder = disabled_repo.validate_token(Some("invalid-value")).unwrap();

                assert_eq!(placeholder.name, "AUTHENTICATION-DISABLED");
                assert_eq!(placeholder.value, "");
                assert_eq!(placeholder.permissions.unwrap().full_access, true);
            }
        }
    }

    mod remove_token {
        use super::*;
        #[rstest]
        fn test_remove_token(mut repo: BoxedTokenRepository) {
            let token = repo.remove_token("test").unwrap();
            assert_eq!(token, ());
        }

        #[rstest]
        fn test_remove_init_token(mut repo: BoxedTokenRepository) {
            let token = repo.remove_token("init-token");
            assert_eq!(
                token,
                Err(conflict!("Can't remove provisioned token 'init-token'"))
            );
        }

        #[rstest]
        fn test_remove_token_not_found(mut repo: BoxedTokenRepository) {
            let token = repo.remove_token("test-1");
            assert_eq!(token, Err(not_found!("Token 'test-1' doesn't exist")));
        }

        #[rstest]
        fn test_remove_token_persistent(path: PathBuf, init_token: &str) {
            let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
            let _ = repo.generate_token(init_token, Permissions::default());
            repo.generate_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            )
            .unwrap();

            repo.remove_token("test").unwrap();

            let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
            let token = repo.get_token("test");

            assert_eq!(token, Err(not_found!("Token 'test' doesn't exist")));
        }

        #[rstest]
        fn test_remove_token_no_init_token(mut disabled_repo: BoxedTokenRepository) {
            let token = disabled_repo.remove_token("test");
            assert_eq!(token, Ok(()));
        }

        #[rstest]
        fn test_remove_provisioned_token(mut repo: BoxedTokenRepository) {
            let token = repo.get_mut_token("test").unwrap();
            token.is_provisioned = true;

            let err = repo.remove_token("test").err().unwrap();
            assert_eq!(err, conflict!("Can't remove provisioned token 'test'"))
        }
    }

    mod rename_bucket {
        use super::*;

        #[rstest]
        fn test_rename_bucket(mut repo: BoxedTokenRepository) {
            repo.generate_token(
                "test-2",
                Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-1".to_string()],
                },
            )
            .expect("Failed to generate token");

            repo.rename_bucket("bucket-1", "bucket-2").unwrap();

            let token = repo.get_token("test-2").unwrap();
            let permissions = token.permissions.as_ref().unwrap();

            assert_eq!(permissions.read, vec!["bucket-2".to_string()]);
            assert_eq!(permissions.write, vec!["bucket-2".to_string()]);
        }

        #[rstest]
        fn test_rename_bucket_not_exit(mut repo: BoxedTokenRepository) {
            repo.rename_bucket("bucket-1", "bucket-2").unwrap();

            let token = repo.get_token("test").unwrap();
            let permissions = token.permissions.as_ref().unwrap();

            assert!(permissions.read.is_empty());
            assert!(permissions.write.is_empty());
        }

        #[rstest]
        fn test_rename_bucket_persistent(path: PathBuf, init_token: &str) {
            let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
            let _ = repo.generate_token(init_token, Permissions::default());
            repo.generate_token(
                "test-2",
                Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-1".to_string()],
                },
            )
            .expect("Failed to generate token");

            repo.rename_bucket("bucket-1", "bucket-2").unwrap();

            let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
            let token = repo.get_token("test-2").unwrap();
            let permissions = token.permissions.as_ref().unwrap();

            assert_eq!(permissions.read, vec!["bucket-2".to_string()]);
            assert_eq!(permissions.write, vec!["bucket-2".to_string()]);
        }

        #[rstest]
        fn test_rename_bucket_no_init_token(mut disabled_repo: BoxedTokenRepository) {
            let result = disabled_repo.rename_bucket("bucket-1", "bucket-2");
            assert!(result.is_ok());
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    fn init_token() -> &'static str {
        "init-token"
    }

    #[fixture]
    fn repo(path: PathBuf, init_token: &str) -> BoxedTokenRepository {
        let mut repo = TokenRepositoryBuilder::new(Default::default()).build(path.clone());
        let _ = repo.generate_token(
            init_token,
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );
        let _ = repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );
        repo
    }

    #[fixture]
    fn disabled_repo(path: PathBuf) -> BoxedTokenRepository {
        TokenRepositoryBuilder::new(Default::default()).build(path.clone())
    }
}
