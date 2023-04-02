// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::core::status::{HTTPError};

pub trait AssetManager {
    fn read(&self, relative_path: &str) -> Result<String, HTTPError>;

}

/**    fn read(self, relative_path: &str) -> Result<String, HTTPError> {

* Asset manager that reads files from a zip archive as hex string and returns them as string
*/
pub struct ZipAssetManager {}

/**
* Empty asset manager, used when no static files are supported
*/
pub struct EmptyAssetManager {}


impl AssetManager for EmptyAssetManager {
    fn read(&self, _: &str) -> Result<String, HTTPError> {
        Err(HTTPError::not_found("No static files supported"))
    }
}

impl AssetManager for ZipAssetManager {
    fn read(&self, relative_path: &str) -> Result<String, HTTPError> {
        Ok("")
    }
}
