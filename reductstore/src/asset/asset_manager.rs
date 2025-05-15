// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use bytes::Bytes;
use log::{debug, trace};
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::PathBuf;
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

    /// Get the absolute path of a file extracted from the zip archive.
    fn absolut_path(&self, relative_path: &str) -> Result<PathBuf, ReductError>;
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

    fn absolut_path(&self, relative_path: &str) -> Result<PathBuf, ReductError> {
        let path = self.path.path().join(relative_path);
        if !path.try_exists()? {
            return Err(ReductError::not_found(
                format!("File {:?} not found", path).as_str(),
            ));
        }
        Ok(path)
    }
}

/// Empty asset manager that does not support any files
struct NoAssetManager;

impl ManageStaticAsset for NoAssetManager {
    fn read(&self, _relative_path: &str) -> Result<Bytes, ReductError> {
        Err(ReductError::not_found("No static files supported"))
    }

    fn absolut_path(&self, _relative_path: &str) -> Result<PathBuf, ReductError> {
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
    use rstest::rstest;

    mod empty {
        use super::*;
        use reduct_base::not_found;
        #[test]
        fn test_empty_asset_manager() {
            let asset_manager = create_asset_manager(&[]);
            assert_eq!(
                asset_manager.read("test"),
                Err(not_found!("No static files supported"))
            );
        }

        #[test]
        fn test_absolute_path() {
            let asset_manager = create_asset_manager(&[]);
            assert_eq!(
                asset_manager.absolut_path("test"),
                Err(not_found!("No static files supported"))
            );
        }
    }

    mod zip_asset {
        use super::*;
        use reduct_base::error::ErrorCode;
        use rstest::fixture;
        use std::fs;
        use std::fs::File;
        use std::io::Write;
        use zip::write::{ExtendedFileOptions, FileOptions};
        use zip::ZipWriter;

        #[rstest]
        fn test_absolute_path(zip_file: PathBuf) {
            let asset_manager = ZipAssetManager::new(fs::read(zip_file).unwrap().as_slice());
            assert_eq!(
                asset_manager.absolut_path("").unwrap(),
                asset_manager.path.path()
            );
        }

        #[rstest]
        fn test_absolute_path_not_found(zip_file: PathBuf) {
            let asset_manager = ZipAssetManager::new(fs::read(zip_file).unwrap().as_slice());
            assert_eq!(
                asset_manager.absolut_path("xxx.txt").unwrap_err().status,
                ErrorCode::NotFound
            );
        }

        #[rstest]
        fn test_read(zip_file: PathBuf) {
            let asset_manager = ZipAssetManager::new(fs::read(zip_file).unwrap().as_slice());
            let content = asset_manager.read("test.txt").unwrap();
            assert_eq!(content, Bytes::from("test"));
        }

        #[fixture]
        fn zip_file() -> PathBuf {
            let archive = tempdir().unwrap().keep().join("test.zip");
            let mut file = File::create(archive.clone()).unwrap();
            file.write_all(b"test").unwrap();

            // crete zip file
            let mut zip = ZipWriter::new(file);

            zip.start_file::<&str, ExtendedFileOptions>("test.txt", FileOptions::default())
                .unwrap();
            zip.write_all(b"test").unwrap();
            zip.finish().unwrap();
            archive
        }
    }
}
