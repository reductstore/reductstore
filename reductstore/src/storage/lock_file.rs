// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::{FileWeak, FILE_CACHE};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::SeekFrom::Start;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub enum RecoveryStrategy {
    /// No locking
    None,
    /// File-based locking
    File,
}
pub struct LockFile {
    path: PathBuf,
    timeout: Duration,
}

#[derive(Default)]
pub struct LockFileBuilder {
    path: Option<PathBuf>,
    timeout: Option<Duration>,
}

impl LockFileBuilder {
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn build(self) -> LockFile {
        let path = self.path.expect("Lock file path must be set");
        let timeout = self.timeout.unwrap_or(Duration::from_secs(10));
        LockFile { path, timeout }
    }
}

impl LockFile {
    pub fn builder() -> LockFileBuilder {
        LockFileBuilder::default()
    }

    pub async fn acquire(&self) -> Result<(), ReductError> {
        // Check if the file is already locked
        let time_start = std::time::Instant::now();
        while FILE_CACHE.try_exists(&self.path)? {
            if time_start.elapsed() > self.timeout {
                return Err(internal_server_error!("Timeout while waiting for lock"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let file = FILE_CACHE.write_or_create(&self.path, Start(0))?;
        file.upgrade()?.write().unwrap().sync_all()?;
        Ok(())
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _ = FILE_CACHE.remove(&self.path);
    }
}
