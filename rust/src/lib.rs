// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub mod core;
pub mod asset;
pub mod auth;

use crate::core::env::{Env, new_env};
use crate::core::logger::{init_log};
use crate::asset::asset_manager::{ZipAssetManager, new_asset_manager};
use crate::core::status::HTTPError;

#[cxx::bridge(namespace = "reduct::core")]
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
    }
}

#[cxx::bridge(namespace = "reduct::asset")]
mod ffi_asset {
    extern "Rust" {
        type ZipAssetManager;
        fn new_asset_manager(zipped_content: &str) -> Box<ZipAssetManager>;
        fn read(&self, path: &str) -> Result<String>;
    }
}
