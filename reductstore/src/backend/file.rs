// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::backend::BoxedBackend;
use log::debug;
use std::fs::File as StdFile;
use std::fs::OpenOptions as StdOpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(PartialEq, Clone, Debug)]
pub enum AccessMode {
    Read,
    ReadWrite,
}

pub struct File {
    inner: StdFile,
    backend: Arc<BoxedBackend>,
    path: PathBuf,
    last_synced: Instant,
    is_synced: bool,
    mode: AccessMode,
    ignore_write: bool, // read-only mode, write operations are ignored
}

pub struct OpenOptions {
    inner: StdOpenOptions,
    backend: Arc<BoxedBackend>,
    create: bool,
    mode: AccessMode,
    ignore_write: bool,
}

impl OpenOptions {
    pub(crate) fn new(backend: Arc<BoxedBackend>) -> Self {
        Self {
            inner: StdOpenOptions::new(),
            backend,
            create: false,
            mode: AccessMode::Read,
            ignore_write: false,
        }
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.inner.read(read);
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.inner.write(write);
        if write {
            self.mode = AccessMode::ReadWrite;
        }
        self
    }

    pub fn ignore_write(&mut self, ignor_write: bool) -> &mut Self {
        self.ignore_write = ignor_write;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.inner.create(create);
        self.create = create;
        if create {
            self.mode = AccessMode::ReadWrite;
        }
        self
    }

    pub async fn open<P: AsRef<std::path::Path>>(&mut self, path: P) -> std::io::Result<File> {
        if self.ignore_write {
            self.inner.write(false);
            self.inner.create(false);
        }

        let full_path = self.backend.path().join(path.as_ref());
        if !full_path.exists() {
            // the call initiates downloading the file from remote storage if needed
            if self.backend.try_exists(&full_path).await? {
                self.backend.download(&full_path).await?;
            } else if !self.create {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File {:?} does not exist", full_path),
                ));
            }
        }

        let file = self.inner.open(full_path.clone())?;
        Ok(File {
            inner: file,
            backend: Arc::clone(&self.backend),
            path: full_path,
            last_synced: Instant::now(),
            is_synced: true,
            mode: self.mode.clone(),
            ignore_write: self.ignore_write,
        })
    }
}

impl File {
    pub async fn sync_all(&mut self) -> std::io::Result<()> {
        if self.ignore_write {
            return Ok(());
        }

        if self.is_synced() {
            return Ok(());
        }

        debug!("File {} synced to storage backend", self.path.display());

        self.inner.sync_all()?;
        self.backend.upload(&self.path).await?;
        self.last_synced = Instant::now();
        self.is_synced = true;
        Ok(())
    }
    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.inner.metadata()
    }

    pub fn set_len(&mut self, size: u64) -> std::io::Result<()> {
        if self.ignore_write {
            return Ok(());
        }

        self.is_synced = false;
        self.inner.set_len(size)
    }

    // Specifically for cache management
    pub fn last_synced(&self) -> std::time::Instant {
        self.last_synced
    }

    pub fn is_synced(&self) -> bool {
        self.is_synced
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn mode(&self) -> &AccessMode {
        &self.mode
    }

    pub async fn access(&self) -> std::io::Result<()> {
        self.backend
            .update_local_cache(&self.path, &self.mode)
            .await
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.ignore_write {
            return Ok(buf.len());
        }
        self.is_synced = false;
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.ignore_write {
            return Ok(());
        }
        self.inner.flush()
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ObjectMetadata, StorageBackend};
    use async_trait::async_trait;
    use mockall::mock;
    use rstest::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    mod open_options {
        use super::*;
        use std::fs;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_open_options_read(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();
            let copy_path = path.clone();

            // check if file exists
            mock_backend
                .expect_try_exists()
                .times(1)
                .returning(|_| Ok(true));
            // download because it does not exist in cache
            mock_backend.expect_download().times(1).returning(move |p| {
                assert_eq!(p, copy_path.join("non-existing.txt").as_path());
                fs::create_dir_all(p.parent().unwrap()).unwrap();
                fs::write(&copy_path.join("non-existing.txt"), "content").unwrap();
                Ok(())
            });

            let file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .read(true)
                .open("non-existing.txt")
                .await
                .unwrap();

            assert_eq!(file.mode(), &AccessMode::Read);
            assert!(file.is_synced());
            assert_eq!(file.path(), &path.join("non-existing.txt"));
            assert_eq!(file.metadata().unwrap().len(), 7);
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_open_options_read_existing(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();

            // no download because it exists in cache
            mock_backend.expect_download().times(0);

            let file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .read(true)
                .open("test.txt")
                .await
                .unwrap();

            assert_eq!(file.mode(), &AccessMode::Read);
            assert!(file.is_synced());
            assert_eq!(file.path(), &path.join("test.txt"));
            assert_eq!(file.metadata().unwrap().len(), 7);
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_open_options_create_ignore_file_not_exist(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();
            let copy_path = path.clone();

            mock_backend
                .expect_try_exists()
                .times(1)
                .returning(move |p| {
                    assert_eq!(p, copy_path.join("new_file.txt").as_path());
                    Ok(false)
                });
            mock_backend.expect_download().times(0);

            let file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .write(true)
                .create(true)
                .open("new_file.txt")
                .await
                .unwrap();

            assert_eq!(file.mode(), &AccessMode::ReadWrite);
            assert!(file.is_synced());
            assert_eq!(file.path(), &path.join("new_file.txt"));
            assert_eq!(file.metadata().unwrap().len(), 0);
        }
    }

    mod sync {
        use super::*;
        use std::io::Write;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_file_sync_all(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();

            // expect upload when syncing
            mock_backend.expect_upload().times(1).returning(move |p| {
                assert_eq!(p, path.join("test.txt").as_path());
                Ok(())
            });

            let mut file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .write(true)
                .open("test.txt")
                .await
                .unwrap();

            assert!(file.is_synced());
            file.write_all(b" more").unwrap();
            assert!(!file.is_synced());
            file.sync_all().await.unwrap();
            assert!(file.is_synced());
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_is_sync_after_write(mock_backend: MockBackend) {
            let mut file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .write(true)
                .open("test.txt")
                .await
                .unwrap();

            assert!(file.is_synced());
            file.write_all(b" more").unwrap();
            assert!(!file.is_synced());
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_is_sync_after_set_len(mock_backend: MockBackend) {
            let mut file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .write(true)
                .open("test.txt")
                .await
                .unwrap();

            assert!(file.is_synced());
            file.set_len(10).unwrap();
            assert!(!file.is_synced());
        }
    }

    mod access {
        use super::*;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_file_access_read(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();

            // expect update_local_cache when accessing
            mock_backend
                .expect_update_local_cache()
                .times(1)
                .returning(move |p, mode| {
                    assert_eq!(p, path.join("test.txt").as_path());
                    assert_eq!(mode, &AccessMode::Read);
                    Ok(())
                });

            let file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .read(true)
                .open("test.txt")
                .await
                .unwrap();

            file.access().await.unwrap();
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_file_access_read_write(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();

            // expect update_local_cache when accessing
            mock_backend
                .expect_update_local_cache()
                .times(1)
                .returning(move |p, mode| {
                    assert_eq!(p, path.join("test.txt").as_path());
                    assert_eq!(mode, &AccessMode::ReadWrite);
                    Ok(())
                });

            let file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .write(true)
                .open("test.txt")
                .await
                .unwrap();

            file.access().await.unwrap();
        }
    }

    mod read_only {
        use super::*;
        use std::io::Write;

        #[rstest]
        #[tokio::test]
        async fn test_file_write_ignored(mut mock_backend: MockBackend) {
            let path = mock_backend.path().to_path_buf();

            // no upload expected because write is ignored
            mock_backend.expect_upload().times(0);

            let mut file = OpenOptions::new(Arc::new(Box::new(mock_backend)))
                .read(true)
                .write(true)
                .create(true)
                .ignore_write(true)
                .open("test.txt")
                .await
                .unwrap();

            assert!(file.is_synced());
            let bytes_written = file.write(b" more").unwrap();
            assert_eq!(bytes_written, 5);
            assert!(file.is_synced());
            file.sync_all().await.unwrap();
            assert!(file.is_synced());

            // check that the file content is unchanged
            let content = fs::read_to_string(path.join("test.txt")).unwrap();
            assert_eq!(content, "content");
        }
    }

    mock! {
        pub Backend {}

        #[async_trait]
        impl StorageBackend for Backend {
            fn path(&self) -> &PathBuf;
            async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
            async fn remove(&self, path: &Path) -> std::io::Result<()>;
            async fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;
            async fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;
            async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>>;
            async fn try_exists(&self, path: &Path) -> std::io::Result<bool>;
            async fn upload(&self, path: &Path) -> std::io::Result<()>;
            async fn download(&self, path: &Path) -> std::io::Result<()>;
            async fn update_local_cache(&self, path: &Path, mode: &AccessMode) -> std::io::Result<()>;
            async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;
            async fn get_stats(&self, path: &Path) -> std::io::Result<Option<ObjectMetadata>>;
            async fn remove_from_local_cache(&self, path: &Path) -> std::io::Result<()>;
        }

    }

    #[fixture]
    fn mock_backend(path: PathBuf) -> MockBackend {
        // create the file in cache
        fs::create_dir_all(path.as_path()).unwrap();
        fs::write(&path.join("test.txt"), "content").unwrap();

        let mut backend = MockBackend::new();
        backend.expect_path().return_const(path.clone());
        backend
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }
}
