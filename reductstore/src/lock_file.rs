// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FILE_CACHE;
use async_trait::async_trait;
use log::{debug, error, info, warn};
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

#[derive(Debug, PartialEq, Clone, Default)]
pub enum InstanceRole {
    #[default]
    Primary,
    Secondary,
}

#[async_trait]
pub trait LockFile {
    async fn is_locked(&self) -> bool;
    async fn is_failed(&self) -> bool;
    async fn is_waiting(&self) -> bool;
    fn release(&self);
}
pub type BoxedLockFile = Box<dyn LockFile + Sync + Send>;

const WRITE_INTERVAL: Duration = Duration::from_secs(1);
const READ_INTERVAL: Duration = Duration::from_secs(1);

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
    ttl: Option<Duration>,
    failure_action: FailureAction,
    role: InstanceRole,
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

    pub fn with_role(mut self, role: InstanceRole) -> Self {
        self.role = role;
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
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
        let failure_action = self.failure_action;
        let role = self.role;
        let ttl = self.ttl.unwrap_or(Duration::from_secs(0));

        // for future use, we generate a unique id for the lock file
        let unique_id = format!("{}-{}", std::process::id(), uuid::Uuid::new_v4());
        let handle = tokio::spawn(async move {
            // Main loop to acquire and maintain the lock
            loop {
                // Check if the file is already locked
                let time_start = std::time::Instant::now();
                while FILE_CACHE.try_exists(&file_path).unwrap_or(true)
                    && !stop_flag.load(std::sync::atomic::Ordering::SeqCst)
                {
                    if let Some(last_modified) =
                        FILE_CACHE.last_modified(&file_path).unwrap_or(None)
                    {
                        if last_modified.elapsed().unwrap() > ttl && ttl.as_secs() > 0 {
                            warn!(
                                "Lock file is stale (last modified over {:?} ago), removing: {:?}",
                                last_modified.elapsed().unwrap(),
                                file_path
                            );
                            let _ = FILE_CACHE.remove(&file_path);
                            break;
                        }
                    }

                    if time_start.elapsed() > timeout && timeout.as_secs() > 0 {
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
                    tokio::time::sleep(READ_INTERVAL).await;
                }

                // Final check to see if we should stop
                if stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }

                match role {
                    InstanceRole::Primary => {
                        // Primary instance acquires the lock immediately
                        info!("Primary instance acquiring lock file: {:?}", file_path);
                        *state_clone.write().await = State::Locked;
                    }
                    InstanceRole::Secondary => {
                        // Secondary instance waits a bit to ensure the primary has created the lock file
                        tokio::time::sleep(WRITE_INTERVAL * 3).await;
                        if !FILE_CACHE.try_exists(&file_path).unwrap() {
                            info!("Secondary instance acquiring lock file: {:?}", file_path);
                            *state_clone.write().await = State::Locked;
                        } else {
                            info!("Secondary instance could not acquire lock file (already held by primary): {:?}", file_path);
                        }
                    }
                }

                if *state_clone.read().await == State::Locked {
                    break;
                }
            }

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

                tokio::time::sleep(WRITE_INTERVAL).await;
            }
        });

        Box::new(ImplLockFile {
            path,
            stop_on_drop,
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

        debug!("Releasing lock file: {:?}", self.path);
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
