// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FILE_CACHE;
use async_trait::async_trait;
use log::{debug, error};
use reduct_base::error::ReductError;
use std::io::SeekFrom::Start;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Debug, PartialEq)]
pub enum State {
    Waiting,
    Locked,
    Failed,
}

#[async_trait]
pub trait LockFile {
    async fn is_locked(&self) -> bool;
    async fn is_failed(&self) -> bool;
    async fn is_waiting(&self) -> bool;
    fn release(&self);
}
pub type BoxedLockFile = Box<dyn LockFile + Sync + Send>;

struct ImplLockFile {
    path: PathBuf,
    timeout: Duration,
    stop_on_drop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
    state: Arc<RwLock<State>>,
}

#[derive(Default)]
pub struct LockFileBuilder {
    path: Option<PathBuf>,
    lock_timeout: Option<Duration>,
    keep_alive_interval: Option<Duration>,
}

impl LockFileBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_path(mut self, path: &PathBuf) -> Self {
        self.path = Some(path.clone());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = Some(timeout);
        self
    }

    pub fn build(self) -> BoxedLockFile {
        let Some(path) = self.path else {
            return Box::new(NoopLockFile {});
        };

        let timeout = self.lock_timeout.unwrap_or(Duration::from_secs(10));

        let stop_on_drop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop_on_drop);
        let file_path = path.clone();
        let state = Arc::new(RwLock::new(State::Waiting));
        let state_clone = Arc::clone(&state);

        // for future use, we generate a unique id for the lock file
        let unique_id = format!("{}-{}", std::process::id(), uuid::Uuid::new_v4());
        let handle = tokio::spawn(async move {
            // Check if the file is already locked
            let time_start = std::time::Instant::now();
            while FILE_CACHE.try_exists(&file_path).unwrap()
                && !stop_flag.load(std::sync::atomic::Ordering::SeqCst)
            {
                if time_start.elapsed() > timeout {
                    error!("Timeout while waiting for lock file: {:?}", file_path);
                    *state_clone.write().await = State::Failed;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            *state_clone.write().await = State::Locked;

            // we need to sync the file to ensure that the lock is visible to other processes
            // With Blue/Green deployments, the file system is shared between instances
            // so we need to keep the file locked as long as the process is running and recreate it if it gets deleted
            // during deployments
            while !stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
                let recreate = async {
                    let rc = FILE_CACHE.write_or_create(&file_path, Start(0))?;
                    let file = rc.upgrade()?;
                    file.write()?.write_all(unique_id.as_bytes())?;
                    file.write()?.sync_all()?;
                    Ok::<(), ReductError>(())
                };

                if let Err(e) = recreate.await {
                    error!("Error while recreating lock file: {}", e);
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        Box::new(ImplLockFile {
            path,
            timeout,
            stop_on_drop: Arc::new(AtomicBool::new(false)),
            handle,
            state,
        })
    }
}

impl ImplLockFile {}

#[async_trait]
impl LockFile for ImplLockFile {
    async fn is_locked(&self) -> bool {
        let state = self.state.read().await;
        *state == State::Locked
    }

    async fn is_failed(&self) -> bool {
        let state = self.state.read().await;
        *state == State::Failed
    }

    async fn is_waiting(&self) -> bool {
        let state = self.state.read().await;
        *state == State::Waiting
    }

    fn release(&self) {
        self.stop_on_drop
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let start_time = std::time::Instant::now();
        while !self.handle.is_finished() && start_time.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(100));
        }

        debug!("Releasing lock file: {:?}", self.stop_on_drop);
        let _ = FILE_CACHE.remove(&self.path);
    }
}

impl Drop for ImplLockFile {
    fn drop(&mut self) {
        self.release()
    }
}

struct NoopLockFile;

#[async_trait]
impl LockFile for NoopLockFile {
    async fn is_locked(&self) -> bool {
        true
    }

    async fn is_failed(&self) -> bool {
        false
    }

    async fn is_waiting(&self) -> bool {
        false
    }

    fn release(&self) {}
}
