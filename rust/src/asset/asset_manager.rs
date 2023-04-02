// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fs::File;
use zip::ZipArchive;
use tempfile::{tempdir, TempDir};
use std::io::{Cursor, Read};
use log::{debug, trace};
use hex;

use crate::core::status::{HTTPError};


/**
 * Asset manager that reads files from a zip archive as hex string and returns them as string
 */
pub struct ZipAssetManager {
    path: Option<TempDir>,
}

impl ZipAssetManager {
    /**
     * Create a new zip asset manager.
     *
     * @param zipped_content The zip archive as hex string.
     * @return The asset manager.
     */
    pub fn new(zipped_content: &str) -> ZipAssetManager {
        if zipped_content.len() == 0 {
            return ZipAssetManager {
                path: None,
            };
        }
        // Convert hex string to binary and extract zip archive into a temporary directory

        if zipped_content.len() % 2 != 0 {
            panic!("Hex string must have even length");
        }


        let binary = hex::decode(zipped_content).expect("Could not decode hex string");
        let cursor = Cursor::new(binary);

        // Create a zip archive from the binary
        let mut archive = ZipArchive::new(cursor).unwrap();
        let temp_dir = tempdir().expect("Could not create temporary directory");

        trace!("Extracting zip archive to {:?}", temp_dir.path());

        let mut root = String::new();
        for i in 0..archive.len()
        {
            let mut file = archive.by_index(i).unwrap();
            if file.is_dir() {
                if root.len() == 0 {
                    root = String::from(file.name());
                } else {
                    // if root is already set, create a subdirectory
                    let path = temp_dir.path().join(file.name()[root.len()..].to_string());
                    std::fs::create_dir_all(path).unwrap();
                }
            }

            if file.is_file() {
                // extract file to temporary directory without root directory
                let path = temp_dir.path().join(file.name()[root.len()..].to_string());
                debug!("Extracting file to {:?}", path);
                let mut out = File::create(path).unwrap();
                std::io::copy(&mut file, &mut out).unwrap();
            }
        }

        ZipAssetManager {
            path: Some(temp_dir),
        }
    }

    /**
     * Read a file from extracted zip archive
     * @param relative_path relative path to file
     */
    pub fn read(&self, relative_path: &str) -> Result<String, HTTPError> {
        if self.path.is_none() {
            return Err(HTTPError::not_found("No static files supported"));
        }

        // check if file exists
        let path = self.path.as_ref().unwrap().path().join(relative_path);

        trace!("Reading file {:?}", path);
        if !path.exists() {
            return Err(HTTPError::not_found(format!("File {:?} not found", path).as_str()));
        }

        // read file
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        Ok(contents)
    }
}

// C++ integration

pub fn new_asset_manager(zipped_content: &str) -> Box<ZipAssetManager> {
    Box::new(ZipAssetManager::new(zipped_content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::status::HTTPError;

    #[test]
    fn test_empty_asset_manager() {
        let asset_manager = ZipAssetManager::new("");
        assert!(asset_manager.read("test") == Err(HTTPError::not_found("No static files supported")));
    }
}
