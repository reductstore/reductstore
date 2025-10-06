// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::{FileWeak, FILE_CACHE};
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::SeekFrom::Start;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::sleep;
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
    stop_on_drop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
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

    pub async fn try_build(self) -> Result<LockFile, ReductError> {
        let path = self.path.expect("Lock file path must be set");
        let timeout = self.timeout.unwrap_or(Duration::from_secs(10));

        // Check if the file is already locked
        let time_start = std::time::Instant::now();
        while FILE_CACHE.try_exists(&path)? {
            if time_start.elapsed() > timeout {
                return Err(internal_server_error!("Timeout while waiting for lock"));
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        let stop_on_drop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop_on_drop);
        let file_path = path.clone();

        let handle = tokio::spawn(async move {
            // we need to sync the file to ensure that the lock is visible to other processes
            // With Blue/Green deployments, the file system is shared between instances
            // so we need to keep the file locked as long as the process is running and recreate it if it gets deleted
            // during deployments
            while !stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
                let recreate = async {
                    let rc = FILE_CACHE.write_or_create(&file_path, Start(0))?;
                    let mut file = rc.upgrade()?;
                    file.write()?.set_len(0)?;
                    file.write()?.sync_all()?;
                    Ok::<(), ReductError>(())
                };

                if let Err(e) = recreate.await {
                    error!("Error while recreating lock file: {}", e);
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        Ok(LockFile {
            path,
            timeout,
            stop_on_drop: Arc::new(AtomicBool::new(false)),
            handle,
        })
    }
}

impl LockFile {
    pub fn builder() -> LockFileBuilder {
        LockFileBuilder::default()
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        self.stop_on_drop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        while !self.handle.is_finished() {
            sleep(Duration::from_millis(100));
        }

        debug!("Releasing lock file: {:?}", self.stop_on_drop);
        let _ = FILE_CACHE.remove(&self.path);
    }
}
