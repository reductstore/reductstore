// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FILE_CACHE;
use async_trait::async_trait;
use log::{debug, error, warn};
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

#[derive(Debug, PartialEq, Clone, Default)]
pub enum FailureAction {
    Proceed,
    #[default]
    Abort,
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
    stop_on_drop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
    state: Arc<RwLock<State>>,
}

#[derive(Default)]
pub struct LockFileBuilder {
    path: Option<PathBuf>,
    lock_timeout: Option<Duration>,
    failure_action: FailureAction,
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

    pub fn with_failure_action(mut self, action: FailureAction) -> Self {
        self.failure_action = action;
        self
    }

    pub fn build(self) -> BoxedLockFile {
        let Some(path) = self.path else {
            return Box::new(NoopLockFile {});
        };

        let timeout = self.lock_timeout.unwrap_or(Duration::from_secs(10));
        // Atomic flag to signal the background task to stop
        let stop_on_drop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop_on_drop);
        let file_path = path.clone();
        let state = Arc::new(RwLock::new(State::Waiting));
        let state_clone = Arc::clone(&state);
        let failure_action = self.failure_action.clone();

        // for future use, we generate a unique id for the lock file
        let unique_id = format!("{}-{}", std::process::id(), uuid::Uuid::new_v4());
        let handle = tokio::spawn(async move {
            // Check if the file is already locked
            let time_start = std::time::Instant::now();
            while FILE_CACHE.try_exists(&file_path).unwrap()
                && !stop_flag.load(std::sync::atomic::Ordering::SeqCst)
            {
                if time_start.elapsed() > timeout {
                    match failure_action {
                        FailureAction::Proceed => {
                            warn!(
                                "Timeout while waiting for lock file, proceeding anyway: {:?}",
                                file_path
                            );
                            break;
                        }
                        FailureAction::Abort => {
                            error!(
                                "Timeout while waiting for lock file, aborting: {:?}",
                                file_path
                            );
                            *state_clone.write().await = State::Failed;
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use rstest::{fixture, rstest};
    use std::fs;
    use tempfile::tempdir;
    use tokio::time::error::Elapsed;
    use tokio::time::timeout;

    #[rstest]
    #[tokio::test]
    async fn test_lock_file_acquire_and_release(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new().with_path(&lock_file_path).build();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await);

        // Wait for the lock to be acquired
        let acquired = wait_new_state(&lock_file).await;

        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await);
        assert!(!lock_file.is_failed().await);
        assert!(!lock_file.is_waiting().await);

        assert_ne!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must be overwritten"
        );

        lock_file.release();
        assert!(
            !lock_file_path.exists(),
            "Lock file must be deleted on release"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_lock_file_timeout_abort(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new()
            .with_path(&lock_file_path)
            .with_timeout(Duration::from_secs(2))
            .build();
        fs::write(&lock_file_path, "dummy").unwrap();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await);

        // Wait for the lock to fail due to timeout
        let failed = wait_new_state(&lock_file).await;
        assert!(failed.is_ok(), "Lock file did not fail in time");
        assert!(lock_file.is_failed().await);
        assert!(!lock_file.is_locked().await);
        assert!(!lock_file.is_waiting().await);

        assert_eq!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must not be overwritten"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_lock_file_timeout_proceed(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new()
            .with_path(&lock_file_path)
            .with_timeout(Duration::from_secs(2))
            .with_failure_action(FailureAction::Proceed)
            .build();
        fs::write(&lock_file_path, "dummy").unwrap();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await);

        // Wait for the lock to be acquired despite the timeout
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await);
        assert!(!lock_file.is_failed().await);
        assert!(!lock_file.is_waiting().await);

        assert_ne!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must be overwritten"
        );
    }

    async fn wait_new_state(lock_file: &BoxedLockFile) -> Result<(), Elapsed> {
        let acquired = timeout(Duration::from_secs(10), async {
            loop {
                if !lock_file.is_waiting().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        acquired
    }

    #[fixture]
    fn lock_file_path() -> PathBuf {
        let dir = tempdir().unwrap().keep();
        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(dir.clone())
                .try_build()
                .unwrap(),
        );

        let filepath = dir.join("test.lock");
        filepath
    }
}
