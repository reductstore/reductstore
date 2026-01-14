// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use async_trait::async_trait;
use log::{debug, error, info, warn};
use reduct_base::error::ReductError;
use std::io::SeekFrom::Start;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

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
    async fn is_locked(&self) -> Result<bool, ReductError>;
    async fn is_failed(&self) -> Result<bool, ReductError>;
    async fn is_waiting(&self) -> Result<bool, ReductError>;
    async fn release(&self);
}
pub type BoxedLockFile = Box<dyn LockFile + Sync + Send>;
struct ImplLockFile {
    path: PathBuf,
    stop_on_drop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
    state: Arc<AsyncRwLock<State>>,
}

pub(crate) struct LockFileBuilder {
    path_buf: PathBuf,
    config: Cfg,
}
impl LockFileBuilder {
    pub fn noop() -> BoxedLockFile {
        Box::new(NoopLockFile {})
    }

    pub fn new(path_buf: PathBuf) -> Self {
        Self {
            path_buf,
            config: Cfg::default(),
        }
    }

    pub fn with_config(mut self, config: Cfg) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> BoxedLockFile {
        Self::from_config(self.path_buf, self.config)
    }

    fn from_config(path: PathBuf, cfg: Cfg) -> BoxedLockFile {
        let role = cfg.role;
        let cfg = cfg.lock_file_config;

        // Atomic flag to signal the background task to stop
        let stop_on_drop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop_on_drop);
        let file_path = path.clone();
        let state = Arc::new(AsyncRwLock::new(State::Waiting));
        let state_clone = Arc::clone(&state);

        let mut this = Box::new(ImplLockFile {
            path,
            stop_on_drop,
            handle: tokio::spawn(async {}),
            state,
        });

        let handle = tokio::spawn(async move {
            if let Err(err) =
                Self::run_lock_task(file_path, cfg, role, state_clone, stop_flag).await
            {
                error!("Lock file task failed: {}", err);
            }
        });

        this.handle = handle;
        this
    }

    async fn run_lock_task(
        file_path: PathBuf,
        cfg: crate::cfg::lock_file::LockFileConfig,
        role: InstanceRole,
        state: Arc<AsyncRwLock<State>>,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<(), ReductError> {
        // for future use, we generate a unique id for the lock file
        let unique_id = format!("{}-{}", std::process::id(), uuid::Uuid::new_v4());

        // Main loop to acquire and maintain the lock
        loop {
            // Check if the file is already locked
            let time_start = std::time::Instant::now();
            while FILE_CACHE.try_exists(&file_path).await?
                && !stop_flag.load(std::sync::atomic::Ordering::SeqCst)
            {
                if let Some(last_modified) = FILE_CACHE
                    .get_stats(&file_path)
                    .await?
                    .and_then(|meta| meta.modified_time)
                {
                    // elapsed can fail if system time is changed backwards, so we default to 0 duration
                    if last_modified.elapsed().unwrap_or(Duration::from_secs(0)) > cfg.ttl
                        && cfg.ttl.as_secs() > 0
                    {
                        warn!(
                            "Lock file is stale (last modified over {:?} ago), removing: {:?}",
                            last_modified.elapsed().unwrap(),
                            file_path
                        );
                        if let Err(err) = FILE_CACHE.remove(&file_path).await {
                            error!("Failed to remove stale lock file: {:?}", err);
                        }
                        break;
                    }
                }

                if time_start.elapsed() > cfg.timeout && cfg.timeout.as_secs() > 0 {
                    match cfg.failure_action {
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
                            *state.write().await? = State::Failed;
                            return Ok(());
                        }
                    }
                }
                tokio::time::sleep(cfg.polling_interval).await;
            }

            // Final check to see if we should stop
            if stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            match role {
                InstanceRole::Primary => {
                    // Primary instance acquires the lock immediately
                    info!("Primary instance acquiring lock file: {:?}", file_path);
                    *state.write().await? = State::Locked;
                }
                InstanceRole::Secondary => {
                    // Secondary instance waits a bit to ensure the primary has created the lock file
                    tokio::time::sleep(cfg.polling_interval * 3).await;
                    if !FILE_CACHE.try_exists(&file_path).await? {
                        info!("Secondary instance acquiring lock file: {:?}", file_path);
                        *state.write().await? = State::Locked;
                    } else {
                        info!("Secondary instance could not acquire lock file (already held by primary): {:?}", file_path);
                    }
                }
                InstanceRole::Replica | InstanceRole::Standalone => {}
            }

            if *state.read().await? == State::Locked {
                break;
            }
        }

        // we need to sync the file to ensure that the lock is visible to other processes
        // With Blue/Green deployments, the file system is shared between instances
        // so we need to keep the file locked as long as the process is running and recreate it if it gets deleted
        // during deployments
        while !stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
            let recreate = async {
                let mut file = FILE_CACHE.write_or_create(&file_path, Start(0)).await?;
                file.write_all(unique_id.as_bytes())?;
                file.sync_all().await?;
                Ok::<(), ReductError>(())
            };

            if let Err(e) = recreate.await {
                error!("Error while recreating lock file: {}", e);
            }

            tokio::time::sleep(cfg.polling_interval).await;
        }

        Ok(())
    }
}

impl ImplLockFile {}

#[async_trait]
impl LockFile for ImplLockFile {
    async fn is_locked(&self) -> Result<bool, ReductError> {
        Ok(*self.state.read().await? == State::Locked)
    }

    async fn is_failed(&self) -> Result<bool, ReductError> {
        Ok(*self.state.read().await? == State::Failed)
    }

    async fn is_waiting(&self) -> Result<bool, ReductError> {
        Ok(*self.state.read().await? == State::Waiting)
    }

    async fn release(&self) {
        self.stop_on_drop
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let start_time = std::time::Instant::now();
        while !self.handle.is_finished() && start_time.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(100)).await;
        }

        debug!("Releasing lock file: {:?}", self.path);
        let path = self.path.clone();

        // Try to remove the file, but don't error if it doesn't exist (already removed)
        match FILE_CACHE.remove(&path).await {
            Ok(_) => {}
            Err(err) if err.status == reduct_base::error::ErrorCode::NotFound => {
                // File already removed, this is okay
            }
            Err(err)
                if err.status == reduct_base::error::ErrorCode::InternalServerError
                    && err.message.contains("cannot find the file") =>
            {
                // File already removed on Windows, this is okay
            }
            Err(err) => {
                error!("Failed to remove lock file: {:?}", err);
            }
        }
    }
}

impl Drop for ImplLockFile {
    fn drop(&mut self) {
        self.stop_on_drop
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let path = self.path.clone();
        // Use block_in_place to handle async cleanup in drop
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = std::thread::spawn(move || {
                handle.block_on(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    // Try to remove the file, but don't error if it doesn't exist (already removed)
                    match FILE_CACHE.remove(&path).await {
                        Ok(_) => {}
                        Err(err) if err.status == reduct_base::error::ErrorCode::NotFound => {
                            // File already removed, this is okay
                        }
                        Err(err)
                            if err.status == reduct_base::error::ErrorCode::InternalServerError
                                && err.message.contains("cannot find the file") =>
                        {
                            // File already removed on Windows, this is okay
                        }
                        Err(err) => {
                            error!("Failed to remove lock file: {:?}", err);
                        }
                    }
                });
            })
            .join();
        }
    }
}

struct NoopLockFile;

#[async_trait]
impl LockFile for NoopLockFile {
    async fn is_locked(&self) -> Result<bool, ReductError> {
        Ok(true)
    }

    async fn is_failed(&self) -> Result<bool, ReductError> {
        Ok(false)
    }

    async fn is_waiting(&self) -> Result<bool, ReductError> {
        Ok(false)
    }

    async fn release(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::lock_file::LockFileConfig;
    use crate::cfg::{Cfg, InstanceRole};
    use rstest::{fixture, rstest};
    use std::fs;
    use tempfile::tempdir;
    use tokio::time::error::Elapsed;
    use tokio::time::timeout;

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_acquire_and_release(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone()).build();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to be acquired
        let acquired = wait_new_state(&lock_file).await;

        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());
        assert!(!lock_file.is_failed().await.unwrap());
        assert!(!lock_file.is_waiting().await.unwrap());

        assert_ne!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must be overwritten"
        );

        lock_file.release().await;
        assert!(
            !lock_file_path.exists(),
            "Lock file must be deleted on release"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_timeout_abort(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    timeout: Duration::from_secs(2),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();
        fs::write(&lock_file_path, "dummy").unwrap();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to fail due to timeout
        let failed = wait_new_state(&lock_file).await;
        assert!(failed.is_ok(), "Lock file did not fail in time");
        assert!(lock_file.is_failed().await.unwrap());
        assert!(!lock_file.is_locked().await.unwrap());
        assert!(!lock_file.is_waiting().await.unwrap());

        assert_eq!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must not be overwritten"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_timeout_proceed(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    timeout: Duration::from_secs(2),
                    failure_action: FailureAction::Proceed,
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();
        fs::write(&lock_file_path, "dummy").unwrap();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to be acquired despite the timeout
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());
        assert!(!lock_file.is_failed().await.unwrap());
        assert!(!lock_file.is_waiting().await.unwrap());

        assert_ne!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must be overwritten"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_secondary_instance_waits(lock_file_path: PathBuf) {
        let primary_lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();

        let secondary_lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ..Default::default()
                },
                InstanceRole::Secondary,
            ))
            .build();

        // Wait for the primary to acquire the lock
        let primary_acquired = wait_new_state(&primary_lock_file).await;
        assert!(
            primary_acquired.is_ok(),
            "Primary lock file was acquired in time"
        );
        assert!(primary_lock_file.is_locked().await.unwrap());

        // Wait for the secondary to acquire the lock
        let secondary_acquired = wait_new_state(&secondary_lock_file).await;
        assert!(
            secondary_acquired.is_err(),
            "Secondary lock file was not acquired in time"
        );
        assert!(secondary_lock_file.is_waiting().await.unwrap());

        // Release primary lock
        primary_lock_file.release().await;
        let secondary_acquired = wait_new_state(&secondary_lock_file).await;
        assert!(
            secondary_acquired.is_ok(),
            "Secondary lock file was acquired in time"
        );
        assert!(secondary_lock_file.is_locked().await.unwrap());

        secondary_lock_file.release().await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ttl_removes_stale_lock(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ttl: Duration::from_secs(1),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();
        fs::write(&lock_file_path, "dummy").unwrap();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to be acquired after TTL expires
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_secondary_acquires_if_file_missing(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(100),
                    ..Default::default()
                },
                InstanceRole::Secondary,
            ))
            .build();

        // Secondary should grab the lock if file does not exist after grace period.
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Secondary failed to acquire lock");
        assert!(lock_file.is_locked().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_lock_task_secondary_timeout_proceed_does_not_acquire_when_file_exists(
        lock_file_path: PathBuf,
    ) {
        fs::write(&lock_file_path, "dummy").unwrap();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AsyncRwLock::new(State::Waiting));

        let cfg = LockFileConfig {
            polling_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            failure_action: FailureAction::Proceed,
            ..Default::default()
        };

        let task = tokio::spawn(LockFileBuilder::run_lock_task(
            lock_file_path.clone(),
            cfg,
            InstanceRole::Secondary,
            Arc::clone(&state),
            Arc::clone(&stop_flag),
        ));

        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(*state.read().await.unwrap(), State::Waiting);

        stop_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        timeout(Duration::from_secs(2), task)
            .await
            .expect("run_lock_task must stop")
            .unwrap()
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_lock_task_replica_runs_once_and_stops(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);
        let _ = fs::remove_dir_all(&lock_file_path);

        let stop_flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AsyncRwLock::new(State::Waiting));
        let cfg = LockFileConfig {
            polling_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            ..Default::default()
        };

        let task = tokio::spawn(LockFileBuilder::run_lock_task(
            lock_file_path,
            cfg,
            InstanceRole::Replica,
            state,
            Arc::clone(&stop_flag),
        ));

        tokio::task::yield_now().await;
        stop_flag.store(true, std::sync::atomic::Ordering::SeqCst);

        timeout(Duration::from_secs(2), task)
            .await
            .expect("run_lock_task must stop")
            .unwrap()
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_lock_task_recreate_error_when_path_is_directory(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);
        fs::create_dir(&lock_file_path).unwrap();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AsyncRwLock::new(State::Waiting));

        let cfg = LockFileConfig {
            polling_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            failure_action: FailureAction::Proceed,
            ..Default::default()
        };

        let task = tokio::spawn(LockFileBuilder::run_lock_task(
            lock_file_path,
            cfg,
            InstanceRole::Primary,
            Arc::clone(&state),
            Arc::clone(&stop_flag),
        ));

        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(*state.read().await.unwrap(), State::Locked);

        stop_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        timeout(Duration::from_secs(2), task)
            .await
            .expect("run_lock_task must stop")
            .unwrap()
            .unwrap();
    }

    async fn wait_new_state(lock_file: &BoxedLockFile) -> Result<(), Elapsed> {
        let acquired = timeout(Duration::from_secs(10), async {
            loop {
                if !lock_file.is_waiting().await.unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        acquired
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_drops_lock_file(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();

        // Wait for the lock to be acquired
        let acquired = wait_new_state(&lock_file).await;

        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());

        // Drop the lock file, which should trigger release
        drop(lock_file);

        // Wait a moment to ensure the lock file is released
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(
            !lock_file_path.exists(),
            "Lock file must be deleted on drop"
        );
    }

    #[fixture]
    fn lock_file_path() -> PathBuf {
        let dir = tempdir().unwrap().keep();

        let filepath = dir.join("test.lock");
        filepath
    }

    fn test_cfg(lock_file_config: LockFileConfig, role: InstanceRole) -> Cfg {
        Cfg {
            lock_file_config,
            role,
            ..Default::default()
        }
    }
}
