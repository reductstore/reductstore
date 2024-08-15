// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use bytes::Bytes;
use log::{debug, trace};
use std::fs::File;
use std::io::{Cursor, Read};
use tempfile::{tempdir, TempDir};
use zip::ZipArchive;

use reduct_base::error::ReductError;

pub trait ManageStaticAsset {
    /// Read a file from the zip archive.
    ///
    /// # Arguments
    ///
    /// * `relative_path` - The relative path to the file.
    ///
    /// # Returns
    ///
    /// The file content as string.
    fn read(&self, relative_path: &str) -> Result<Bytes, ReductError>;
}

/// Asset manager that reads files from a zip archive as hex string and returns them as string
struct ZipAssetManager {
    path: TempDir,
}

impl ZipAssetManager {
    /// Create a new zip asset manager.
    ///
    /// # Arguments
    ///
    /// * `zipped_content` - The zip archive as hex string. If the string is empty, the asset manager
    /// will not support any files and return 404 for all requests.
    ///
    /// # Returns
    ///
    /// The asset manager.
    ///
    /// # Panics
    ///
    /// If the zip archive is empty.
    pub fn new(zipped_content: &[u8]) -> ZipAssetManager {
        if zipped_content.len() == 0 {
            panic!("Empty zip archive")
        }

        let cursor = Cursor::new(zipped_content);

        // Create a zip archive from the binary
        let mut archive = ZipArchive::new(cursor).unwrap();
        let temp_dir = tempdir().expect("Could not create temporary directory");

        trace!("Extracting zip archive to {:?}", temp_dir.path());

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap();
            if file.is_dir() {
                let path = temp_dir.path().join(file.name());
                std::fs::create_dir_all(path).unwrap();
            }

            if file.is_file() {
                // extract file to temporary directory without root directory
                let path = temp_dir.path().join(file.name());
                debug!("Extracting file to {:?}", path);
                let mut out = File::create(path).unwrap();
                std::io::copy(&mut file, &mut out).unwrap();
            }
        }

        ZipAssetManager { path: temp_dir }
    }
}

impl ManageStaticAsset for ZipAssetManager {
    fn read(&self, relative_path: &str) -> Result<Bytes, ReductError> {
        // check if file exists
        let path = self.path.path().join(relative_path);

        trace!("Reading file {:?}", path);
        if !path.try_exists()? {
            return Err(ReductError::not_found(
                format!("File {:?} not found", path).as_str(),
            ));
        }

        // read file
        let mut file = File::open(path)?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;

        Ok(Bytes::from(content))
    }
}

/// Empty asset manager that does not support any files
struct NoAssetManager;

impl ManageStaticAsset for NoAssetManager {
    fn read(&self, _relative_path: &str) -> Result<Bytes, ReductError> {
        Err(ReductError::not_found("No static files supported"))
    }
}

pub fn create_asset_manager(zipped_content: &[u8]) -> Box<dyn ManageStaticAsset + Send + Sync> {
    if zipped_content.len() == 0 {
        Box::new(NoAssetManager)
    } else {
        Box::new(ZipAssetManager::new(zipped_content))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::error::ReductError;

    #[test]
    fn test_empty_asset_manager() {
        let asset_manager = create_asset_manager(&[]);
        assert!(
            asset_manager.read("test") == Err(ReductError::not_found("No static files supported"))
        );
    }
}
