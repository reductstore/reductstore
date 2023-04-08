// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub mod core;
pub mod asset;
pub mod auth;

use std::path::PathBuf;
use serde_json;

use crate::core::env::{Env, new_env};
use crate::core::logger::{init_log};
use crate::core::status::{HTTPStatus, HTTPError};

use crate::asset::asset_manager::{ZipAssetManager, new_asset_manager};
use crate::auth::policy::{AnonymousPolicy, AuthenticatedPolicy, FullAccessPolicy, Policy, ReadAccessPolicy, WriteAccessPolicy};

use crate::auth::token_repository::*;
use crate::auth::token_auth::*;
use crate::auth::proto::{Token, TokenCreateResponse};
use crate::auth::proto::token::Permissions;

type DynPolicy = Box<dyn Policy>;

#[cxx::bridge(namespace = "reduct::rust_part")]
mod ffi_core {
    extern "Rust" {
        type Env;
        fn new_env() -> Box<Env>;
        fn get_string(&mut self, key: &str, default_value: &str, masked: bool) -> String;
        fn get_int(&mut self, key: &str, default_value: i32, masked: bool) -> i32;
        fn message(&self) -> &str;
    }

    extern "Rust" {
        fn init_log(level: &str);
    }

    extern "Rust" {
        type HTTPError;
        fn status(self: &HTTPError) -> i32;
        fn message(self: &HTTPError) -> &str;
    }

    extern "Rust" {
        type ZipAssetManager;
        fn new_asset_manager(zipped_content: &str) -> Box<ZipAssetManager>;
        fn read(&self, path: &str) -> Result<String>;
    }

    extern "Rust" {
        type TokenCreateResponse;
        fn new_token_create_response() -> Box<TokenCreateResponse>;
        fn token_create_response_to_json(resp: &TokenCreateResponse) -> String;
    }

    extern "Rust" {
        type Permissions;
        fn new_token_permissions(full_access: bool, read: Vec<String>, write: Vec<String>) -> Box<Permissions>;
        fn json_to_token_permissions(json: &str, permissions: &mut Permissions) -> Box<HTTPError>;
        fn token_permissions_read(permissions: &Permissions) -> Vec<String>;
        fn token_permissions_write(permissions: &Permissions) -> Vec<String>;
        fn token_permissions_to_json(permissions: &Permissions) -> String;
    }

    extern "Rust" {
        type TokenRepository;
        type Token;
        fn new_token_repo(config_path: &str, init_token: &str) -> Box<TokenRepository>;
        fn token_repo_create_token(repo: &mut TokenRepository,
                                   name: &str,
                                   permissions: &Permissions,
                                   resp: &mut TokenCreateResponse) -> Box<HTTPError>;
        fn token_repo_remove_bucket_from_tokens(repo: &mut TokenRepository, bucket: &str) -> Box<HTTPError>;
        fn token_repo_validate_token(repo: &TokenRepository, value: &str, resp: &mut Token) -> Box<HTTPError>;
        fn token_repo_get_token(repo: &TokenRepository, name: &str, token: &mut Token) -> Box<HTTPError>;
        fn token_repo_get_token_list(repo: &TokenRepository, tokens: &mut Vec<Token>) -> Box<HTTPError>;
        fn token_repo_remove_token(repo: &mut TokenRepository, name: &str) -> Box<HTTPError>;

        fn new_token() -> Box<Token>;
        fn token_to_json(token: &Token) -> String;
        fn token_list_to_json(tokens: &Vec<Token>) -> String;
    }

    extern "Rust" {
        type TokenAuthorization;
        type DynPolicy;

        fn auth_check(auth: &TokenAuthorization, header: &str, repo: &TokenRepository, policy: &DynPolicy) -> Box<HTTPError>;
        fn new_anonymous_policy() ->Box<DynPolicy>;
        fn new_authenticated_policy() -> Box<DynPolicy>;
        fn new_full_access_policy() -> Box<DynPolicy>;
        fn new_read_policy(bucket: &str) -> Box<DynPolicy>;
        fn new_write_policy(bucket: &str) -> Box<DynPolicy>;
    }
}

// C++ interface
pub fn new_token_repo(config_path: &str, init_token: &str) -> Box<TokenRepository> {
    let config_path = PathBuf::from(config_path);
    let init_token = if init_token.is_empty() {
        None
    } else {
        Some(init_token.to_string())
    };
    Box::new(TokenRepository::new(config_path, init_token))
}

pub fn new_token_permissions(full_access: bool, read: Vec<String>, write: Vec<String>) -> Box<Permissions> {
    Box::new(Permissions {
        full_access,
        read,
        write,
    })
}

pub fn token_repo_create_token(repo: &mut TokenRepository, name: &str, permissions: &Permissions, resp: &mut TokenCreateResponse) -> Box<HTTPError> {
    let err = match repo.create_token(name, permissions.clone()) {
        Ok(token) => {
            *resp = token;
            HTTPError::ok()
        }
        Err(err) => err,
    };
    Box::new(err)
}

pub fn token_repo_remove_bucket_from_tokens(repo: &mut TokenRepository, bucket: &str) -> Box<HTTPError> {
    let err = match repo.remove_bucket_from_tokens(bucket) {
        Ok(_) => HTTPError::ok(),
        Err(err) => err,
    };
    Box::new(err)
}

pub fn token_repo_validate_token(repo: &TokenRepository, value: &str, resp: &mut Token) -> Box<HTTPError> {
    let err = match repo.validate_token(value) {
        Ok(token) => {
            *resp = token;
            HTTPError::ok()
        }
        Err(err) => err,
    };
    Box::new(err)
}

pub fn token_repo_get_token(repo: &TokenRepository, name: &str, token: &mut Token) -> Box<HTTPError> {
    let err = match repo.find_by_name(name) {
        Ok(t) => {
            *token = t;
            HTTPError::ok()
        }
        Err(err) => err,
    };
    Box::new(err)
}

pub fn token_repo_get_token_list(repo: &TokenRepository, tokens: &mut Vec<Token>) -> Box<HTTPError> {
    let err = match repo.get_token_list() {
        Ok(list) => {
            *tokens = list;
            HTTPError::ok()
        }
        Err(err) => err,
    };
    Box::new(err)
}

pub fn token_repo_remove_token(repo: &mut TokenRepository, name: &str) -> Box<HTTPError> {
    let err = match repo.remove_token(name) {
        Ok(_) => HTTPError::ok(),
        Err(err) => err,
    };
    Box::new(err)
}

pub fn new_token_create_response() -> Box<TokenCreateResponse> {
    Box::new(TokenCreateResponse::default())
}

pub fn token_create_response_to_json(resp: &TokenCreateResponse) -> String {
    serde_json::to_string(resp).unwrap()
}

pub fn new_token() -> Box<Token> {
    Box::new(Token::default())
}

pub fn token_to_json(token: &Token) -> String {
    serde_json::to_string(token).unwrap()
}

pub fn token_list_to_json(tokens: &Vec<Token>) -> String {
    serde_json::to_string(tokens).unwrap()
}

pub fn json_to_token_permissions(json: &str, permissions: &mut Permissions) -> Box<HTTPError> {
    let err = match serde_json::from_str(json) {
        Ok(p) => {
            *permissions = p;
            HTTPError::ok()
        }
        Err(err) => HTTPError::new(HTTPStatus::BadRequest, err.to_string().as_str()),
    };
    Box::new(err)
}

pub fn token_permissions_to_json(permissions: &Permissions) -> String {
    serde_json::to_string(permissions).unwrap()
}

pub fn token_permissions_read(permissions: &Permissions) -> Vec<String> {
    permissions.read.clone()
}

pub fn token_permissions_write(permissions: &Permissions) -> Vec<String> {
    permissions.write.clone()
}

pub fn auth_check(auth: &TokenAuthorization, header: &str, repo: &TokenRepository, policy: &DynPolicy) -> Box<HTTPError> {
    let err = match auth.check(header, repo, policy.as_ref()) {
        Ok(_) => HTTPError::ok(),
        Err(err) => err,
    };
    Box::new(err)
}


pub fn new_anonymous_policy() -> Box<DynPolicy> {
    Box::new(Box::new(AnonymousPolicy{}))
}

pub fn new_authenticated_policy() -> Box<DynPolicy> {
    Box::new(Box::new(AuthenticatedPolicy{}))
}

pub fn new_full_access_policy() -> Box<DynPolicy> {
    Box::new(Box::new(FullAccessPolicy{}))
}

pub fn new_read_policy(bucket: &str) -> Box<DynPolicy> {
    Box::new(Box::new(ReadAccessPolicy{
        bucket: bucket.to_string(),
    }))
}

pub fn new_write_policy(bucket: &str) -> Box<DynPolicy> {
    Box::new(Box::new(WriteAccessPolicy{
        bucket: bucket.to_string(),
    }))
}
